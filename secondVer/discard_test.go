package kv_engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDisCard_listenUpdateds(t *testing.T) {
	path := filepath.Join("/tmp", "kv_engine")

	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer os.RemoveAll(path)

	writeCount := 600000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	// delete
	for i := 0; i < 300000; i++ {
		err := db.Delete(GetKey(i))
		assert.Nil(t, err)
	}

	ccl, err := db.discards[String].getCCL(10, 0.001)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ccl))
}

func TestDiscard_newDiscard(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		path := filepath.Join("/tmp", "kv_engine_discard")
		os.MkdirAll(path, os.ModePerm)
		defer os.RemoveAll(path)
		discard, err := newDiscard(path, discardFileName, 4096)
		assert.Nil(t, err)

		assert.Equal(t, 682, len(discard.freeList))
		assert.Equal(t, 0, len(discard.location))
	})

	t.Run("with-data", func(t *testing.T) {
		path := filepath.Join("/tmp", "kv_engine_discard")
		os.MkdirAll(path, os.ModePerm)
		defer os.RemoveAll(path)
		discard, err := newDiscard(path, discardFileName, 4096)
		assert.Nil(t, err)

		for i := 1; i < 300; i *= 5 {
			discard.setTotal(uint32(i), 223)
			discard.incrDiscard(uint32(i), i*10)
		}

		assert.Equal(t, 678, len(discard.freeList))
		assert.Equal(t, 4, discard.location)

		// reopen
		dis2, err := newDiscard(path, discardFileName, 4096)
		assert.Nil(t, err)
		assert.Equal(t, 678, len(dis2.freeList))
		assert.Equal(t, 4, len(dis2.location))
	})

}

func TestDiscard_clear(t *testing.T) {
	path := filepath.Join("/tmp", "kv_engine_discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)

	dis, err := newDiscard(path, discardFileName, 4096)
	assert.Nil(t, err)

	for i := 0; i < 682; i++ {
		dis.setTotal(uint32(2), uint32(i+100))
		dis.incrDiscard(uint32(i), i+10)
	}

	type args struct {
		fid uint32
	}

	tests := []struct {
		name string
		dis  *discard
		args args
	}{
		{
			"0", dis, args{0},
		},
		{
			"33", dis, args{33},
		},
		{
			"198", dis, args{198},
		},
		{
			"340", dis, args{340},
		},
		{
			"680", dis, args{680},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.dis.clear(tt.args.fid)
		})
		// clear 一次，freeList 长度增加一个
		assert.Equal(t, i+1, len(tt.dis.freeList))
	}

}

func TestDiscard_increDiscard(t *testing.T) {
	path := filepath.Join("/tmp", "kv_engine_discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)

	dis, err := newDiscard(path, discardFileName, 4096)
	assert.Nil(t, err)

	for i := 1; i < 600; i *= 5 {
		dis.setTotal(uint32(i-1), uint32(i+1000))
	}

	for i := 1; i < 600; i *= 5 {
		dis.incrDiscard(uint32(i-1), i+100)
	}

	ccl, err := dis.getCCL(10, 0.0000001)
	assert.Nil(t, err)

	assert.Equal(t, 4, len(ccl))
}

func TestDiscard_getCCL(t *testing.T) {
	path := filepath.Join("/tmp", "kv_engine_discard")
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)
	dis, err := newDiscard(path, discardFileName, 4096)
	assert.Nil(t, err)

	for i := 1; i < 2000; i *= 5 {
		dis.setTotal(uint32(i-1), uint32(i+1000))
	}

	for i := 1; i < 2000; i *= 5 {
		dis.incrDiscard(uint32(i-1), i+100)
	}

	t.Run("normal", func(t *testing.T) {
		ccl, err := dis.getCCL(624, 0.000001)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(ccl))
	})

	t.Run("filter-some", func(t *testing.T) {
		ccl, err := dis.getCCL(100, 0.2)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(ccl))
	})

	t.Run("clear and get", func(t *testing.T) {
		dis.clear(124)
		ccl, err := dis.getCCL(100, 0.0001)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(ccl))
	})
}
