package kv_engine

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoseDB_LPush(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPush(t, true, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPush(t, true, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_RPush(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPush(t, false, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPush(t, false, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_Push_UntilRotateFile(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	writeCount := 600000
	key := []byte("mylist")
	for i := 0; i <= writeCount; i++ {
		err := db.LPush(key, GetValue128B())
		assert.Nil(t, err)
	}
}

func testRoseDBPush(t *testing.T, isLush bool, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil-value", db, args{key: GetKey(0), values: [][]byte{GetValue16B()}}, false,
		},
		{
			"one-value", db, args{key: GetKey(1), values: [][]byte{GetValue16B()}}, false,
		},
		{
			"multi-value", db, args{key: GetKey(2), values: [][]byte{GetValue16B(), GetValue16B(), GetValue16B()}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isLush {
				if err := tt.db.LPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err := tt.db.RPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestRoseDB_LPop(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBLPop(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBLPop(t, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_RPop(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBRPop(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBRPop(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBLPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	listKey := []byte("my_list")
	pop, err := db.LPop(listKey)
	assert.Nil(t, pop)
	assert.Nil(t, err)

	// one
	err = db.LPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v1, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v1)

	// rpush one
	err = db.RPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v2, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v2)

	//	multi
	err = db.LPush(listKey, GetKey(0), GetKey(1), GetKey(2))
	assert.Nil(t, err)

	var values [][]byte
	for db.LLen(listKey) > 0 {
		v, err := db.LPop(listKey)
		assert.Nil(t, err)
		values = append(values, v)
	}
	expected := [][]byte{GetKey(2), GetKey(1), GetKey(0)}
	assert.Equal(t, expected, values)
}

func testRoseDBRPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	listKey := []byte("my_list")
	pop, err := db.RPop(listKey)
	assert.Nil(t, pop)
	assert.Nil(t, err)

	// one
	err = db.RPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v1, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v1)

	// lpush one
	err = db.LPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v2, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v2)

	//	multi
	err = db.RPush(listKey, GetKey(0), GetKey(1), GetKey(2))
	assert.Nil(t, err)

	var values [][]byte
	for db.LLen(listKey) > 0 {
		v, err := db.RPop(listKey)
		assert.Nil(t, err)
		values = append(values, v)
	}
	expected := [][]byte{GetKey(2), GetKey(1), GetKey(0)}
	assert.Equal(t, expected, values)
}

func TestRoseDB_LLen(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	listKey := []byte("my_list")
	err = db.LPush(listKey, GetValue16B(), GetValue16B(), GetValue16B())
	assert.Nil(t, err)
	assert.Equal(t, 3, db.LLen(listKey))

	// close and reopen
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	err = db2.LPush(listKey, GetValue16B(), GetValue16B(), GetValue16B())
	assert.Nil(t, err)
	assert.Equal(t, 6, db2.LLen(listKey))
}

func TestRoseDB_DiscardStat_List(t *testing.T) {
	helper := func(isDelete bool) {
		path := filepath.Join("/tmp", "rosedb")
		opts := DefaultOptions(path)
		opts.LogFileSizeThreshold = 64 << 20
		db, err := Open(opts)
		assert.Nil(t, err)
		defer destroyDB(db)

		listKey := []byte("my_list")
		writeCount := 800000
		for i := 0; i < writeCount; i++ {
			err := db.LPush(listKey, GetKey(i))
			assert.Nil(t, err)
		}

		for i := 0; i < writeCount/3; i++ {
			if i%2 == 0 {
				_, err := db.LPop(listKey)
				assert.Nil(t, err)
			} else {
				_, err := db.RPop(listKey)
				assert.Nil(t, err)
			}
		}

		_ = db.Sync()
		ccl, err := db.discards[List].getCCL(10, 0.2)
		t.Log(err)
		t.Log(ccl)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(ccl))
	}

	t.Run("delete", func(t *testing.T) {
		helper(true)
	})
}

func TestRoseDB_ListGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 64 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	listKey := []byte("my_list")
	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := db.LPush(listKey, GetKey(i))
		assert.Nil(t, err)
	}

	for i := 0; i < writeCount/3; i++ {
		if i%2 == 0 {
			_, err := db.LPop(listKey)
			assert.Nil(t, err)
		} else {
			_, err := db.RPop(listKey)
			assert.Nil(t, err)
		}
	}

	l1 := db.LLen(listKey)
	assert.Equal(t, writeCount-writeCount/3, l1)

	err = db.RunLogFileGC(List, 0, 0.3)
	assert.Nil(t, err)

	l2 := db.LLen(listKey)
	assert.Equal(t, writeCount-writeCount/3, l2)
}

func TestRoseDB_LRange(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBLRange(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBLRange(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBLRange(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key   []byte
		start int
		end   int
	}

	listKey := []byte("my_list")
	// prepare List
	err = db.LPush(listKey, []byte("zero"))
	assert.Nil(t, err)
	err = db.LPush(listKey, []byte("negative one"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("one"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("two"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("three"))
	assert.Nil(t, err)

	tests := []struct {
		name       string
		db         *RoseDB
		args       args
		wantValues [][]byte
		wantErr    bool
	}{
		{
			"nil-key", db, args{key: nil, start: 0, end: 3}, [][]byte(nil), true,
		},
		{
			"start==end", db, args{key: listKey, start: 1, end: 1}, [][]byte{[]byte("zero")}, false,
		},
		{
			"start==end==tailSeq", db, args{key: listKey, start: 4, end: 4}, [][]byte{[]byte("three")}, false,
		},
		{
			"end reset to endSeq", db, args{key: listKey, start: 0, end: 8},
			[][]byte{[]byte("negative one"), []byte("zero"), []byte("one"), []byte("two"), []byte("three")}, false,
		},
		{
			"start and end reset", db, args{key: listKey, start: -100, end: 100},
			[][]byte{[]byte("negative one"), []byte("zero"), []byte("one"), []byte("two"), []byte("three")}, false,
		},
		{
			"start negative end postive", db, args{key: listKey, start: -4, end: 2},
			[][]byte{[]byte("zero"), []byte("one")}, false,
		},
		{
			"start out of range", db, args{key: listKey, start: 5, end: 10}, [][]byte(nil), true,
		},
		{
			"end out of range", db, args{key: listKey, start: 1, end: -8}, [][]byte(nil), true,
		},
		{
			"end lager than start", db, args{key: listKey, start: -1, end: 1}, [][]byte(nil), true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, actualErr := tt.db.LRange(tt.args.key, tt.args.start, tt.args.end)
			assert.Equal(t, tt.wantValues, actual, "acutal is not the same with expected")
			if (actualErr != nil) != tt.wantErr {
				t.Errorf("LRange() error = %v, wantErr %v", actualErr, tt.wantErr)
			}
		})
	}
}

func TestRoseDB_convertLogicalIndexToSeq(t *testing.T) {

	t.Run("fileio", func(t *testing.T) {
		testConvertLogicalIndexToSeq(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testConvertLogicalIndexToSeq(t, MMap, KeyValueMemMode)
	})
}

func testConvertLogicalIndexToSeq(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	listKey := []byte("my_list")
	// prepare List
	err = db.LPush(listKey, []byte("zero"))
	assert.Nil(t, err)
	err = db.LPush(listKey, []byte("negative one"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("one"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("two"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("three"))
	assert.Nil(t, err)

	type args struct {
		key   []byte
		index int
	}

	tests := []struct {
		name     string
		db       *RoseDB
		args     args
		expected uint32
		wantErr  bool
	}{
		{
			"not-exist-key", db, args{key: []byte("not-exist"), index: 0}, uint32(initialListSeq) + 1, false,
		},
		{
			"0", db, args{key: listKey, index: 0}, uint32(initialListSeq - 1), false,
		},
		{
			"negative-1", db, args{key: listKey, index: -3}, uint32(initialListSeq + 1), false,
		},
		{
			"negative-2", db, args{key: listKey, index: -4}, uint32(initialListSeq), false,
		},
		{
			"postive-1", db, args{key: listKey, index: 1}, uint32(initialListSeq), false,
		},
		{
			"postive-2", db, args{key: listKey, index: 3}, uint32(initialListSeq + 2), false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := db.listMeta(tt.args.key)
			assert.Nil(t, err)
			actual, err := tt.db.convertLogicalIndexToSeq(start, end, tt.args.index)
			assert.Equal(t, tt.expected, actual, "expected is not the same with actual")
			if (err != nil) != tt.wantErr {
				t.Errorf("convertLogicalIndexToSeq() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
