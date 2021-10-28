package coordinator

import (
	"fmt"
	"reflect"
)

func interface2int(val interface{}) (int, error) {
	var intVal int
	var err error
	switch t := val.(type) {
	case int:
		intVal = t
	case int8:
		intVal = int(t)
	case int16:
		intVal = int(t)
	case int32:
		intVal = int(t)
	case int64:
		intVal = int(t)
	case uint:
		intVal = int(t)
	case uint8:
		intVal = int(t)
	case uint16:
		intVal = int(t)
	case uint32:
		intVal = int(t)
	case uint64:
		intVal = int(t)
	case float32:
		intVal = int(t)
	case float64:
		intVal = int(t)
	default:
		err = fmt.Errorf("unknown/unsupported data type when converting to int (%v)", reflect.TypeOf(val))
	}
	return intVal, err
}

func interface2uint(val interface{}) (uint, error) {
	var intVal uint
	var err error
	switch t := val.(type) {
	case uint:
		intVal = t
	case int:
		intVal = uint(t)
	case int8:
		intVal = uint(t)
	case int16:
		intVal = uint(t)
	case int32:
		intVal = uint(t)
	case int64:
		intVal = uint(t)
	case uint8:
		intVal = uint(t)
	case uint16:
		intVal = uint(t)
	case uint32:
		intVal = uint(t)
	case uint64:
		intVal = uint(t)
	case float32:
		intVal = uint(t)
	case float64:
		intVal = uint(t)
	default:
		err = fmt.Errorf("unknown/unsupported data type when converting to int (%v)", reflect.TypeOf(val))
	}
	return intVal, err
}
