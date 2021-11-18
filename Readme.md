# nanodm

A nanomsg based data model library.  Allows multiple sources to feed a single data model.


## Overview

Multiple data model `sources` can be implemented and connected to the `coordinator server` to implement a data model.  The coordinator server then knows how to route `Get` and `Set` requests to the source that handles each `data model object`.


## A bit about routing

If the following sources were registered with the coordinator server:

  * Source 1 objects:
     * `Device.DeviceInfo.MemoryStatus.Total`
     * `Device.DeviceInfo.MemoryStatus.Free`
  * Source 2 objects:
     * `Device.WiFi.RadioNumberOfEntries`
     * `Device.WiFi.SSIDNumberOfEntries`
     * `Device.WiFi.Radio.0.Enable`

Any calls to get/set the object `Device.DeviceInfo.MemoryStatus.Total` would be routed to `source 1` and any calls to get/set the object `Device.WiFi.RadioNumberOfEntries` would be routed to `source 2`.


## Source Example

A source must implement the GetObjects/SetObjects/AddRow handlers interface.

```golang
type ExampleSource struct {}

func (ex *ExampleSource) GetObjects(objectNames []string) (objects []nanodm.Object, err error) {
    return objects, err
}

func (ex *ExampleSource) SetObjects(objects []nanodm.Object) error {
    return nil
}

// Called to add a row to a dynamic list.  For example:  Device.NAT.PortMapping.{i}.
func (ex *ExampleSource) AddRow(objects nanodm.Object) error {
    return nil
}

// Called to delete a row to a dynamic list.  For example:  Device.NAT.PortMapping.{i}.
func (ex *ExampleSource) DeleteRow(row nanodm.Object) error {
    return nil
}
```

Start the source to start receiving callbacks on the interface above.

```golang
log := logrus.NewEntry(logrus.New())
sourceName := "ExampleSource"
coordinatorUrl := "tcp://127.0.0.1:4500"
sourceUrl := "tcp://127.0.0.1:4501"
exampleSource := ExampleSource{}

// Create the new source passing the custom SourceHandler
source := source.NewSource(log, sourceName, coordinatorUrl, sourceUrl, exampleSource)
// Connect
source.Connect()

// Register a static DM entry, and a dynamic table/list
exampleObjects := []nanodm.Object{{
		Name:   "Device.DeviceInfo.MemoryStatus.Total",
		Access: nanodm.AccessRO,
		Type:   nanodm.TypeInt,
	},
    {
		Name:   "Device.Custom.Dynamic.",
		Access: nanodm.AccessRW,
		Type:   nanodm.TypeDynamicList,
	},
}
// Call register with a list of objects the source owns
err := source.Register(exampleObjects)

```

Once the example above is registered the server side will route all requests Get
for the object `Device.DeviceInfo.MemoryStatus.Total` to this source.

Further all requests (Set/Get/AddRow/DeleteRow) for `Device.NAT.PortMapping.*` will
be routed to this source.

## Coordinator Server Example

A Coordinator must implement the Registered/Unregistered/UpdateObjects interface.  For example:

```golang
type ExampleCoordinator struct {}

func (ch *ExampleCoordinator) Registered(server *coordinator.Server, sourceName string, objects []nanodm.Object) error {
	return nil
}

func (ch *ExampleCoordinator) Unregistered(server *coordinator.Server, sourceName string, objects []nanodm.Object) error {
	return nil
}

func (ch *ExampleCoordinator) UpdateObjects(server *Server, sourceName string, objects []nanodm.Object, deletedObjects map[string]nanodm.Object) error {
	return nil
}

```

Once the handler interface is implemented the server can be started:

```golang
url := "tcp://127.0.0.1:4500"

log := logrus.NewEntry(logrus.New())

exCoordinator := &ExampleCoordinator{}

server := coordinator.NewServer(log, url, exCoordinator)
err := server.Start()
```

Once the server is running the following APIs can be called to access registered sources:

Get an object (or list objects):

```golang
objs, errs := server.Get([]string{"Device.DeviceInfo.MemoryStatus.Total"})
```

Set an object:

```golang
err := server.Set(nanodm.Object{
    Name:  "Device.WiFi.Radio.0.Enable",
    Value: true,
    Type:  nanodm.TypeString,
})
```

Add a row to a dynamic list of a source:

```golang
// Define the new row
newRow := map[string]interface{}{
    "Description":          "Test",
    "Enable":               "false",
    "ExternalPort":         "210",
    "ExternalPortEndRange": "210",
    "InternalClient":       "10.0.0.48",
    "Protocol":             "BOTH",
}

// Add Row to dynamic list entry
err = server.AddRow(nanodm.Object{
    Name:  "Device.NAT.PortMapping.",
    Value: newRow,
    Type:  nanodm.TypeRow,
})
```

Deletes a row to a dynamic list of a source:

```golang
err = server.DeleteRow(nanodm.Object{
    Name: "Device.NAT.PortMapping.1",
    Type: nanodm.TypeRow,
})
```


## Development

Running tests:

```
go test ./... 
```
