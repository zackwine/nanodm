package coordinator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	i   int   = 42
	i8  int8  = 42
	i16 int16 = 42
	i32 int32 = 42
	i64 int64 = 42

	ui   uint   = 42
	ui8  uint8  = 42
	ui16 uint16 = 42
	ui32 uint32 = 42
	ui64 uint64 = 42

	f32 float32 = 42.1
	f64 float64 = 42.1
)

func TestInterface2int(t *testing.T) {

	var retVal int
	var err error

	retVal, err = interface2int(i8)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(i16)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(i32)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(i64)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(ui8)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(ui16)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(ui32)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(ui64)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(f32)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)

	retVal, err = interface2int(f64)
	assert.Nil(t, err)
	assert.Equal(t, i, retVal)
}

func TestInterface2uint(t *testing.T) {

	var retVal uint
	var err error

	retVal, err = interface2uint(i8)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(i16)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(i32)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(i64)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(ui8)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(ui16)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(ui32)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(ui64)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(f32)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)

	retVal, err = interface2uint(f64)
	assert.Nil(t, err)
	assert.Equal(t, ui, retVal)
}
