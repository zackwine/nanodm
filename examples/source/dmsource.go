package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
		Name:   "Device.DeviceInfo.Serial",
		Access: nanodm.AccessRW,
		Type:   nanodm.TypeString,
	},
}

var objectValuesSource1 = map[string]string{
	"Device.DeviceInfo.Version": "0.0.1",
	"Device.DeviceInfo.Serial":  "0101010101",
}

func getObjectsFromMap(objMap map[string]nanodm.Object) (objects []nanodm.Object) {
	for _, object := range objMap {
		objects = append(objects, object)
	}
	return objects
}

type ExampleSource struct {
	log          *logrus.Entry
	objectMap    map[string]nanodm.Object
	objectValues map[string]string
	objects      []nanodm.Object
}

func (ex *ExampleSource) GetObjects(objectNames []string) (objects []nanodm.Object, err error) {
	var errString string
	ex.log.Infof("[%s] Called GetObjects with objects: %v", objectNames)
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
	ex.log.Infof("[%s] Called SetObjects with objects: %+v", objects)
	return nil
}

func main() {

	url := "tcp://127.0.0.1:4500"
	pullUrl1 := "tcp://127.0.0.1:4501"

	logrus.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})
	logrus.SetLevel(logrus.DebugLevel)
	log := logrus.NewEntry(logrus.New())

	log.Info("Starting dmsource example...")

	// Create an ExampleSource that implements SourceInterface
	example := &ExampleSource{
		log:          log,
		objectMap:    objectMapSource1,
		objectValues: objectValuesSource1,
		objects:      getObjectsFromMap(objectMapSource1),
	}

	// Create the new source passing a custom SourceInterface
	source := source.NewSource(log, "testSource", url, pullUrl1, example)
	// Connect
	source.Connect()
	// Call register to update the list of objects the source owns
	err := source.Register(example.objects)
	if err != nil {
		log.Errorf("Failed to register client objects: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	logrus.Infof("Killed with sig %v", sig)
}
