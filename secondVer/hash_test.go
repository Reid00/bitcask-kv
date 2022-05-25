package kv_engine

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoseDB_HMSet(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHMSet(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBHMSet(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHMSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key []byte
		arg [][]byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil", db, args{key: nil, arg: [][]byte{[]byte("field-0"), []byte("val-0")}}, false,
		},
		{
			"nil-value", db, args{key: GetKey(1), arg: [][]byte{[]byte("field-0"), nil}}, false,
		},
		{
			"wrong-num-of-args", db, args{key: GetKey(2), arg: [][]byte{[]byte("field-0")}}, true,
		},
		{
			"normal-single-pair", db, args{key: GetKey(3), arg: [][]byte{[]byte("field-0"), []byte("val-0")}}, false,
		},
		{
			"normal-mulit-pair", db, args{key: GetKey(4), arg: [][]byte{[]byte("field-0"), []byte("val-0"),
				[]byte("field-1"), []byte("val-1")}}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.HMSet(tt.args.key, tt.args.arg...)
			if (err != nil) != tt.wantErr {
				t.Errorf("HMSet() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrWrongNumberOfArgs) {
				t.Errorf("HMSet() error = %v, expected error = %v", err, ErrWrongNumberOfArgs)
			}
		})
	}

	t.Run("check-key-nil", func(t *testing.T) {
		val, err := db.HGet(nil, []byte("field-0"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("val-0"), val)
	})

	t.Run("check-val-nil", func(t *testing.T) {
		val, err := db.HGet(nil, []byte("field-0"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("val-0"), val)
	})

	t.Run("check-single-field", func(t *testing.T) {
		val, err := db.HGet(GetKey(3), []byte("field-0"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("val-0"), val, "single field not same")
	})

	t.Run("check-mulit-field", func(t *testing.T) {
		value, err := db.HMGet(GetKey(4), []byte("field-0"), []byte("field-1"))
		assert.Nil(t, err)
		assert.Equal(t, [][]byte{[]byte("val-0"), []byte("val-1")}, value, "multi field not same")
	})
}


func TestRoseDB_HSet(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSet(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSet(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	// defer destroyDB(db)

	type args struct {
		key   []byte
		field []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil", db, args{key: nil, field: nil, value: GetKey(123)}, false,
		},
		{
			"nil-value", db, args{key: GetKey(1), field: GetKey(11), value: nil}, false,
		},
		{
			"normal", db, args{key: GetKey(1), field: GetKey(11), value: GetValue16B()}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.HSet(tt.args.key, tt.args.field, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("HSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}