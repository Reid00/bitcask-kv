package kv_engine

import (
	"bytes"
	"fmt"
	"kv_engine/logger"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	path := filepath.Join("/tmp", "kv_engine")

	t.Run("default", func(t *testing.T) {
		opts := DefaultOptions(path)
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})

	// t.Run("mmap", func(t *testing.T) {
	// 	opts := DefaultOptions(path)
	// 	db, err := Open(opts)
	// 	defer destroyDB(db)
	// 	assert.Nil(t, err)
	// 	assert.NotNil(t, db)
	// })
}

func destroyDB(db *RoseDB) {
	if db != nil {
		err := os.RemoveAll(db.opts.DBPath)
		if err != nil {
			logger.Errorf("destory db err: %v", err)
		}
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	// return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
	return []byte(fmt.Sprintf("kvstore-bench-key------%09d", n))
}

func GetValue16B() []byte {
	return GetValue(16)
}

func GetValue128B() []byte {
	return GetValue(128)
}

func GetValue4K() []byte {
	return GetValue(4096)
}

func GetValue(n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteByte(alphabet[rand.Int()%36])
	}
	return buf.Bytes()
}
