package kv_engine

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoseDB_HSet(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSet(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
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
			"nil", db, args{key: nil, arg: [][]byte{[]byte("field-0"), []byte("val-1")}}, false,
		},
		{
			"nil-value", db, args{key: GetKey(1), arg: [][]byte{[]byte("field-0"), nil}}, false,
		},
		{
			"wrong-num-of-args", db, args{key: GetKey(1), arg: [][]byte{[]byte("field-0")}}, true,
		},
		{
			"normal", db, args{key: GetKey(2), arg: [][]byte{[]byte("field-0"), []byte("val-1"), []byte("field-1"), []byte("val-2")}}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.HMSet(tt.args.key, tt.args.arg...); (err != nil) != tt.wantErr {
				t.Errorf("HMSet() error = %v, wantErr %v", err, tt.wantErr)
			}
			val, err := tt.db.HGet(tt.args.key, tt.args.arg[0])
			assert.Nil(t, err)
			assert.Equal(t, tt.args.arg[1], val)
		})
	}
}
