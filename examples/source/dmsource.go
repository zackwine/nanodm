package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
	"github.com/zackwine/nanodm/source"
)

var objectMapSource1 = map[string]nanodm.Object{
	"Device.DeviceInfo.Version": {
		Name:   "Device.DeviceInfo.Version",
		Access: nanodm.AccessRO,
		Type:   nanodm.TypeString,
	},
	"Device.DeviceInfo.Serial": {
		Name:   "Device.DeviceInfo.Serial",
		Access: nanodm.AccessRO,
		Type:   nanodm.TypeString,
	},
	"Device.DeviceInfo.Reboot": {
		Name:   "Device.DeviceInfo.Reboot",
		Access: nanodm.AccessRW,
		Type:   nanodm.TypeBool,
	},
}

var objectValuesSource1 = map[string]interface{}{
	"Device.DeviceInfo.Version": "0.0.1",
	"Device.DeviceInfo.Serial":  "0101010101",
	"Device.DeviceInfo.Reboot":  false,
}

var objectMapSource2 = map[string]nanodm.Object{
	"Device.Custom.Setting1": {
		Name:   "Device.Custom.Setting1",
		Access: nanodm.AccessRW,
		Type:   nanodm.TypeString,
	},
	"Device.Custom.Setting2": {
		Name:   "Device.Custom.Setting2",
		Access: nanodm.AccessRW,
		Type:   nanodm.TypeInt,
	},
	"Device.Custom.Version": {
		Name:   "Device.Custom.Version",
		Access: nanodm.AccessRO,
		Type:   nanodm.TypeString,
	},
}

var objectValuesSource2 = map[string]interface{}{
	"Device.Custom.Setting1": "8.8.8.8",
	"Device.Custom.Setting2": 600,
	"Device.Custom.Version":  "2.3.4",
}

type ExampleSource struct {
	log          *logrus.Entry
	sourceName   string
	objectMap    map[string]nanodm.Object
	objectValues map[string]interface{}
	objects      []nanodm.Object
}

func (ex *ExampleSource) GetObjects(objectNames []string) (objects []nanodm.Object, err error) {
	var errString string
	ex.log.Infof("[%s] Called GetObjects with objects: %v", ex.sourceName, objectNames)
	for _, name := range objectNames {
		if object, ok := ex.objectMap[name]; ok {
			object.Value = ex.objectValues[name]
			objects = append(objects, object)
		} else {
			errString = fmt.Sprintf("%s, '%s'", errString, name)
		}
	}

	if errString != "" {
		err = fmt.Errorf("unable to get objects %s", errString)
	}
	return objects, err
}

func (ex *ExampleSource) SetObjects(objects []nanodm.Object) error {
	ex.log.Infof("[%s] Called SetObjects with objects: %+v", ex.sourceName, objects)
	for _, object := range objects {
		ex.objectValues[object.Name] = object.Value
	}
	return nil
}

func startExampleSource1(log *logrus.Entry, coordinatorUrl string, sourceUrl string, sourceName string) *source.Source {

	// Create an ExampleSource that implements SourceHandler
	example := &ExampleSource{
		log:          log,
		sourceName:   sourceName,
		objectMap:    objectMapSource1,
		objectValues: objectValuesSource1,
		objects:      nanodm.GetObjectsFromMap(objectMapSource1),
	}

	// Create the new source passing a custom SourceHandler
	source := source.NewSource(log, sourceName, coordinatorUrl, sourceUrl, example)
	// Connect
	source.Connect()
	// Call register to update the list of objects the source owns
	err := source.Register(example.objects)
	if err != nil {
		log.Errorf("Failed to register client objects: %v", err)
	}
	return source
}

func startExampleSource2(log *logrus.Entry, coordinatorUrl string, sourceUrl string, sourceName string) *source.Source {

	// Create an ExampleSource that implements SourceHandler
	example := &ExampleSource{
		log:          log,
		sourceName:   sourceName,
		objectMap:    objectMapSource2,
		objectValues: objectValuesSource2,
		objects:      nanodm.GetObjectsFromMap(objectMapSource2),
	}

	// Create the new source passing a custom SourceHandler
	source := source.NewSource(log, sourceName, coordinatorUrl, sourceUrl, example)
	// Connect
	source.Connect()
	// Call register to update the list of objects the source owns
	err := source.Register(example.objects)
	if err != nil {
		log.Errorf("Failed to register client objects: %v", err)
	}
	return source
}

func main() {

	coordinatorUrl := "tcp://127.0.0.1:4500"
	sourceUrl1 := "tcp://127.0.0.1:4501"
	sourceUrl2 := "tcp://127.0.0.1:4502"

	logrus.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})
	logrus.SetLevel(logrus.DebugLevel)
	log := logrus.NewEntry(logrus.New())

	log.Info("Starting dmsource example...")
	source1 := startExampleSource1(log.WithField("handler", "testSource1"), coordinatorUrl, sourceUrl1, "testSource1")
	source2 := startExampleSource2(log.WithField("handler", "testSource2"), coordinatorUrl, sourceUrl2, "testSource2")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	logrus.Infof("Killed with sig %v", sig)

	// Best effort cleanup
	go source1.Disconnect()
	go source2.Disconnect()
	// Give a few seconds for disconnect to clean up
	<-time.After(5 * time.Second)
}
