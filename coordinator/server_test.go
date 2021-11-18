package coordinator

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/zackwine/nanodm"
	"github.com/zackwine/nanodm/source"
)

type TestSource struct {
	log          *logrus.Entry
	objectMap    map[string]nanodm.Object
	objectValues map[string]interface{}
	nextIndex    int
}

func (ts *TestSource) GetObjects(objectNames []string) (objects []nanodm.Object, err error) {
	ts.log.Infof("Calling GetObjects %+v", objectNames)
	var errString string
	for _, name := range objectNames {
		if object, ok := ts.objectMap[name]; ok {

			if object.Type == nanodm.TypeDynamicList {
				for dynObjName, dynObject := range ts.objectMap {
					if strings.HasPrefix(dynObjName, name) && dynObjName != name {
						dynObject.Value = ts.objectValues[dynObjName]
						objects = append(objects, dynObject)
					}
				}

			} else {
				object.Value = ts.objectValues[name]
				objects = append(objects, object)
			}
		} else {
			errString = fmt.Sprintf("%s, '%s'", errString, name)
		}
	}

	if errString != "" {
		err = fmt.Errorf("unable to get objects %s", errString)
		ts.log.Errorf("Failed to find objects %s", errString)
	}
	return objects, err
}

func (ts *TestSource) SetObjects(objects []nanodm.Object) error {
	ts.log.Infof("Calling SetObjects %+v", objects)
	for _, object := range objects {
		ts.objectValues[object.Name] = object.Value
	}
	return nil
}

func (ts *TestSource) AddRow(objects nanodm.Object) error {
	ts.log.Infof("Calling AddRow %+v", objects)

	parameterMap, typeOk := objects.Value.(map[string]interface{})
	if !typeOk {
		ts.log.Errorf("object value type is not map[string]interface{}")
		return fmt.Errorf("object value type is not map[string]interface{}")
	}
	for paramName, paramValue := range parameterMap {
		objName := fmt.Sprintf("%s%d.%s", objects.Name, ts.nextIndex, paramName)
		ts.log.Infof("Adding object %s", objName)
		ts.objectValues[objName] = paramValue
		ts.objectMap[objName] = nanodm.Object{
			Name:   objName,
			Access: nanodm.AccessRW,
			Type:   nanodm.TypeString,
		}
	}
	ts.nextIndex++
	return nil
}

func (ex *TestSource) DeleteRow(row nanodm.Object) error {
	ex.log.Infof("Called DeleteRow for: %s", row.Name)
	var toDeleteObjs []string

	for objName, _ := range ex.objectMap {
		if strings.HasPrefix(objName, row.Name) {
			toDeleteObjs = append(toDeleteObjs, objName)
		}
	}

	for _, objName := range toDeleteObjs {
		ex.log.Infof("Deleting obj (%s)", objName)
		delete(ex.objectMap, objName)
		delete(ex.objectValues, objName)
	}

	return nil
}

type TestCoordinator struct {
	log                 *logrus.Entry
	registeredSource    string
	registeredObjects   []nanodm.Object
	unregisteredSource  string
	unregisteredObjects []nanodm.Object
	updatedSource       string
	updatedObjects      []nanodm.Object
}

func (ch *TestCoordinator) Registered(server *Server, sourceName string, objects []nanodm.Object) error {
	ch.log.Infof("Registered source %s", sourceName)
	ch.registeredSource = sourceName
	ch.registeredObjects = objects
	return nil
}

func (ch *TestCoordinator) Unregistered(server *Server, sourceName string, objects []nanodm.Object) error {
	ch.log.Infof("Unregistered source %s", sourceName)
	ch.unregisteredSource = sourceName
	ch.unregisteredObjects = objects
	return nil
}

func (ch *TestCoordinator) UpdateObjects(server *Server, sourceName string, objects []nanodm.Object, deletedObjects map[string]nanodm.Object) error {
	ch.log.Infof("UpdateObjects called for source %s", sourceName)
	ch.updatedSource = sourceName
	ch.updatedObjects = objects
	return nil
}

func getLogger() *logrus.Entry {
	logrus.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})
	logrus.SetLevel(logrus.DebugLevel)
	return logrus.NewEntry(logrus.New())
}

func TestServerRegistration(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4400"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4401"
	sourceUrl2 := "tcp://127.0.0.1:4499"

	var objectMapSource = map[string]nanodm.Object{
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

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1": "8.8.8.8",
		"Device.Custom.Setting2": 600,
		"Device.Custom.Version":  "2.3.4",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	src := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = src.Connect()
	assert.Nil(t, err)
	defer src.Disconnect()

	err = src.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)
	// Verify the source is registered
	assert.Equal(t, sourceName, testCorrdinator.registeredSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.registeredObjects))

	// Create a source with same name
	testSource2 := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	src2 := source.NewSource(log, sourceName, serverUrl, sourceUrl2, testSource2)
	err = src2.Connect()
	assert.Nil(t, err)

	testCorrdinator.registeredSource = ""
	err = src2.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.NotNil(t, err)
	assert.Equal(t, "", testCorrdinator.registeredSource)
}

func TestServerUnregistration(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4502"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4503"

	var objectMapSource = map[string]nanodm.Object{
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

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1": "8.8.8.8",
		"Device.Custom.Setting2": 600,
		"Device.Custom.Version":  "2.3.4",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	source := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = source.Connect()
	assert.Nil(t, err)
	defer source.Disconnect()

	err = source.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)
	// Verify the source is registered
	assert.Equal(t, sourceName, testCorrdinator.registeredSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.registeredObjects))
	assert.Equal(t, len(objectMapSource), len(server.objects))

	err = source.Unregister()
	assert.Nil(t, err)

	<-time.After(2 * time.Second)
	// Verify the source is unregistered
	assert.Equal(t, sourceName, testCorrdinator.unregisteredSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.unregisteredObjects))
	assert.Equal(t, 0, len(server.objects))
}

func TestServerGet(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4504"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4505"

	var objectMapSource = map[string]nanodm.Object{
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

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1": "8.8.8.8",
		"Device.Custom.Setting2": 600,
		"Device.Custom.Version":  "2.3.4",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	source := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = source.Connect()
	assert.Nil(t, err)
	defer source.Disconnect()

	err = source.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)
	// Verify the source is registered
	assert.Equal(t, sourceName, testCorrdinator.registeredSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.registeredObjects))
	assert.Equal(t, len(objectMapSource), len(server.objects))

	// happy path
	gotObjects, errs := server.Get([]string{"Device.Custom.Setting1", "Device.Custom.Setting2"})
	assert.Equal(t, 0, len(errs))
	assert.Equal(t, 2, len(gotObjects))

	// Invalid object
	gotObjects, errs = server.Get([]string{"Not.Valid"})
	log.Infof("errs: %+v", errs)
	log.Infof("gotObjects: %+v", gotObjects)
	assert.Equal(t, 1, len(errs))
	assert.Equal(t, 0, len(gotObjects))

	for objName, expectedVal := range objectValuesSource {
		retObjs, errs := server.Get([]string{objName})
		assert.Equal(t, 0, len(errs))
		assert.Equal(t, 1, len(retObjs))
		assert.EqualValues(t, expectedVal, retObjs[0].Value)
	}
}

func TestServerSet(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4506"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4507"

	var objectMapSource = map[string]nanodm.Object{
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

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1": "8.8.8.8",
		"Device.Custom.Setting2": 600,
		"Device.Custom.Version":  "2.3.4",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	source := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = source.Connect()
	assert.Nil(t, err)
	defer source.Disconnect()

	err = source.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)
	// Verify the source is registered
	assert.Equal(t, sourceName, testCorrdinator.registeredSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.registeredObjects))
	assert.Equal(t, len(objectMapSource), len(server.objects))

	// happy path
	gotObjects, errs := server.Get([]string{"Device.Custom.Setting1"})
	assert.Equal(t, 0, len(errs))
	assert.Equal(t, 1, len(gotObjects))
	assert.Equal(t, objectValuesSource["Device.Custom.Setting1"], gotObjects[0].Value)

	newValue := "3.3.3.3"
	// Set one of the read-write objects
	err = server.Set(nanodm.Object{
		Name:  "Device.Custom.Setting1",
		Value: newValue,
		Type:  nanodm.TypeString,
	})
	assert.Nil(t, err)

	gotObjects, errs = server.Get([]string{"Device.Custom.Setting1"})
	assert.Equal(t, 0, len(errs))
	assert.Equal(t, 1, len(gotObjects))
	assert.Equal(t, newValue, gotObjects[0].Value)
}

func TestServerUpdate(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4508"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4509"

	var objectMapSource = map[string]nanodm.Object{
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

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1": "8.8.8.8",
		"Device.Custom.Setting2": 600,
		"Device.Custom.Version":  "2.3.4",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	source := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = source.Connect()
	assert.Nil(t, err)
	defer source.Disconnect()

	err = source.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)
	// Verify the source is registered
	assert.Equal(t, sourceName, testCorrdinator.registeredSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.registeredObjects))
	assert.Equal(t, len(objectMapSource), len(server.objects))

	delete(objectMapSource, "Device.Custom.Setting1")
	source.UpdateObjects(nanodm.GetObjectsFromMap(objectMapSource))

	// Give the update a few seconds to take
	<-time.After(2 * time.Second)

	// Verify server callbacks were called
	assert.Equal(t, sourceName, testCorrdinator.updatedSource)
	assert.Equal(t, len(objectMapSource), len(testCorrdinator.updatedObjects))
	assert.Equal(t, len(objectMapSource), len(server.objects))

}

func TestServerClientGet(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4510"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4511"
	sourceName2 := "testSource2"
	sourceUrl2 := "tcp://127.0.0.1:4512"

	var objectMapSource = map[string]nanodm.Object{
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

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1": "8.8.8.8",
		"Device.Custom.Setting2": 600,
		"Device.Custom.Version":  "2.3.4",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    objectMapSource,
		objectValues: objectValuesSource,
	}
	src1 := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = src1.Connect()
	assert.Nil(t, err)
	defer src1.Disconnect()

	err = src1.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	src2 := source.NewSource(log, sourceName2, serverUrl, sourceUrl2, nil)
	err = src2.Connect()
	assert.Nil(t, err)
	defer src2.Disconnect()

	err = src2.Register(nil)
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)

	gotObjects, err := src2.GetObjects([]nanodm.Object{{
		Name: "Device.Custom.Version",
	}})

	log.Infof("gotObjects: %+v", gotObjects)
	// Verify get worked as expected
	assert.Nil(t, err)
	assert.Equal(t, 1, len(gotObjects))
	assert.Equal(t, objectValuesSource["Device.Custom.Version"], gotObjects[0].Value)

}

func TestServerDynamicListGet(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4513"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4514"

	// Used for registration
	var objectMapSource = map[string]nanodm.Object{
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
		"Device.Custom.Dynamic.": {
			Name:   "Device.Custom.Dynamic.",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeDynamicList,
		},
	}

	// Used for returning data
	var dynamicObjectMapSource = map[string]nanodm.Object{
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
		"Device.Custom.Dynamic.": {
			Name:   "Device.Custom.Dynamic.",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeDynamicList,
		},
		"Device.Custom.Dynamic.0.Value1": {
			Name:   "Device.Custom.Dynamic.0.Value1",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.0.Value2": {
			Name:   "Device.Custom.Dynamic.0.Value2",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.1.Value1": {
			Name:   "Device.Custom.Dynamic.1.Value1",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.1.Value2": {
			Name:   "Device.Custom.Dynamic.1.Value2",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
	}

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1":         "8.8.8.8",
		"Device.Custom.Setting2":         600,
		"Device.Custom.Version":          "2.3.4",
		"Device.Custom.Dynamic.0.Value1": "val1",
		"Device.Custom.Dynamic.0.Value2": "val2",
		"Device.Custom.Dynamic.1.Value1": "1val1",
		"Device.Custom.Dynamic.1.Value2": "1val2",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log,
	}
	server := NewServer(log, serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log,
		objectMap:    dynamicObjectMapSource,
		objectValues: objectValuesSource,
	}
	src1 := source.NewSource(log, sourceName, serverUrl, sourceUrl, testSource)
	err = src1.Connect()
	assert.Nil(t, err)
	defer src1.Disconnect()

	err = src1.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)

	objs, errs := server.Get([]string{"Device.Custom.Dynamic."})
	assert.Zero(t, len(errs), fmt.Sprintf("Unexpected errors returned %v", errs))
	assert.NotZero(t, len(objs))
	log.Infof("objs: %+v", objs)
	assert.Equal(t, 4, len(objs))

}

func TestServerDynamicListSet(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4515"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4516"

	// Used for registration
	var objectMapSource = map[string]nanodm.Object{
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
		"Device.Custom.Dynamic.": {
			Name:   "Device.Custom.Dynamic.",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeDynamicList,
		},
	}

	// Used for returning data
	var dynamicObjectMapSource = map[string]nanodm.Object{
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
		"Device.Custom.Dynamic.": {
			Name:   "Device.Custom.Dynamic.",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeDynamicList,
		},
		"Device.Custom.Dynamic.0.Value1": {
			Name:   "Device.Custom.Dynamic.0.Value1",
			Access: nanodm.AccessRW,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.0.Value2": {
			Name:   "Device.Custom.Dynamic.0.Value2",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.1.Value1": {
			Name:   "Device.Custom.Dynamic.1.Value1",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.1.Value2": {
			Name:   "Device.Custom.Dynamic.1.Value2",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
	}

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1":         "8.8.8.8",
		"Device.Custom.Setting2":         600,
		"Device.Custom.Version":          "2.3.4",
		"Device.Custom.Dynamic.0.Value1": "val1",
		"Device.Custom.Dynamic.0.Value2": "val2",
		"Device.Custom.Dynamic.1.Value1": "1val1",
		"Device.Custom.Dynamic.1.Value2": "1val2",
	}

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log.WithField("logger", "TestCoordinator"),
	}
	server := NewServer(log.WithField("logger", "Server"), serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log.WithField("logger", "TestSource"),
		objectMap:    dynamicObjectMapSource,
		objectValues: objectValuesSource,
	}
	src1 := source.NewSource(log.WithField("logger", "Source"), sourceName, serverUrl, sourceUrl, testSource)
	err = src1.Connect()
	assert.Nil(t, err)
	defer src1.Disconnect()

	err = src1.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)

	newValue := "newVal!"
	// Set one of the read-write objects
	err = server.Set(nanodm.Object{
		Name:  "Device.Custom.Dynamic.0.Value1",
		Value: newValue,
		Type:  nanodm.TypeString,
	})
	assert.Nil(t, err)

	objs, errs := server.Get([]string{"Device.Custom.Dynamic.0.Value1"})
	assert.Zero(t, len(errs), fmt.Sprintf("Unexpected errors returned %v", errs))
	assert.NotZero(t, len(objs))
	log.Infof("objs: %+v", objs)
	assert.Equal(t, 1, len(objs))
	assert.Equal(t, newValue, objs[0].Value)
}

func TestServerDynamicListAddAndDeleteRow(t *testing.T) {

	serverUrl := "tcp://127.0.0.1:4517"
	sourceName := "testSource"
	sourceUrl := "tcp://127.0.0.1:4518"

	// Used for registration
	var objectMapSource = map[string]nanodm.Object{
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
		"Device.Custom.Dynamic.": {
			Name:   "Device.Custom.Dynamic.",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeDynamicList,
		},
	}

	// Used for returning data
	var dynamicObjectMapSource = map[string]nanodm.Object{
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
		"Device.Custom.Dynamic.": {
			Name:   "Device.Custom.Dynamic.",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeDynamicList,
		},
		"Device.Custom.Dynamic.0.Value1": {
			Name:   "Device.Custom.Dynamic.0.Value1",
			Access: nanodm.AccessRW,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.0.Value2": {
			Name:   "Device.Custom.Dynamic.0.Value2",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.1.Value1": {
			Name:   "Device.Custom.Dynamic.1.Value1",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
		"Device.Custom.Dynamic.1.Value2": {
			Name:   "Device.Custom.Dynamic.1.Value2",
			Access: nanodm.AccessRO,
			Type:   nanodm.TypeString,
		},
	}

	var objectValuesSource = map[string]interface{}{
		"Device.Custom.Setting1":         "8.8.8.8",
		"Device.Custom.Setting2":         600,
		"Device.Custom.Version":          "2.3.4",
		"Device.Custom.Dynamic.0.Value1": "val1",
		"Device.Custom.Dynamic.0.Value2": "val2",
		"Device.Custom.Dynamic.1.Value1": "1val1",
		"Device.Custom.Dynamic.1.Value2": "1val2",
	}

	nextIndex := 2

	log := getLogger()

	// Create a coordinator server
	testCorrdinator := &TestCoordinator{
		log: log.WithField("logger", "TestCoordinator"),
	}
	server := NewServer(log.WithField("logger", "Server"), serverUrl, testCorrdinator)
	err := server.Start()
	assert.Nil(t, err)
	defer server.Stop()

	// Create a test source
	testSource := &TestSource{
		log:          log.WithField("logger", "TestSource"),
		objectMap:    dynamicObjectMapSource,
		objectValues: objectValuesSource,
		nextIndex:    nextIndex,
	}
	src1 := source.NewSource(log.WithField("logger", "Source"), sourceName, serverUrl, sourceUrl, testSource)
	err = src1.Connect()
	assert.Nil(t, err)
	defer src1.Disconnect()

	err = src1.Register(nanodm.GetObjectsFromMap(objectMapSource))
	assert.Nil(t, err)

	// Give the registration a few seconds to take
	<-time.After(2 * time.Second)

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
		Name:  "Device.Custom.Dynamic.",
		Value: newRow,
		Type:  nanodm.TypeRow,
	})

	assert.Nil(t, err)

	// Check a new index was added
	newIndexObjName := fmt.Sprintf("Device.Custom.Dynamic.%d.Description", nextIndex)
	objs, errs := server.Get([]string{newIndexObjName})
	assert.Zero(t, len(errs), fmt.Sprintf("Unexpected errors returned %v", errs))
	assert.NotZero(t, len(objs))
	log.Infof("objs: %+v", objs)
	assert.Equal(t, 1, len(objs))
	assert.Equal(t, "Test", objs[0].Value)

	newIndexObjName = fmt.Sprintf("Device.Custom.Dynamic.%d.ExternalPort", nextIndex)
	objs, errs = server.Get([]string{newIndexObjName})
	assert.Zero(t, len(errs), fmt.Sprintf("Unexpected errors returned %v", errs))
	assert.NotZero(t, len(objs))
	log.Infof("objs: %+v", objs)
	assert.Equal(t, 1, len(objs))
	assert.Equal(t, "210", objs[0].Value)

	// Delete
	rowObjName := fmt.Sprintf("Device.Custom.Dynamic.%d.", nextIndex)
	err = server.DeleteRow(nanodm.Object{
		Name: rowObjName,
		Type: nanodm.TypeRow,
	})
	assert.Nil(t, err)

	// Verify a get fails on the now removed row entries
	objs, errs = server.Get([]string{newIndexObjName})
	assert.NotZero(t, len(errs), "Should have received an error for deleted entry")

}
