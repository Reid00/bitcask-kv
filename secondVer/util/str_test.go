package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrToFloat64(t *testing.T) {
	val := "3.141516280274"
	res, err := StrToFloat64(val)
	assert.Nil(t, err)
	assert.Equal(t, res, 3.141516280274)
}

func TestFloat64ToStr(t *testing.T) {
	val := 3.141516280274
	res := Float64ToStr(val)
	assert.Equal(t, res, "3.141516280274", "they are should be the same")
}
