package mmap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ONLY TEST on Linux well, not sure why window is not supported

func TestMmap(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "mmap.txt"))
	assert.Nil(t, err)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	assert.Nil(t, err)

	defer func() {
		if f != nil {
			f.Close()
			err := os.Remove(path) // BUG access is denied
			t.Log("os.Remove: ", err)
			assert.Nil(t, err)
		}
	}()

	type args struct {
		fd       *os.File
		writable bool
		size     int64
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"zero-size", args{fd: f, writable: true, size: 0}, true,
		},
		{
			"normal-size", args{fd: f, writable: true, size: 100}, false,
		},
		{
			"big-size", args{fd: f, writable: true, size: 128 << 20}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Mmap(tt.args.fd, tt.args.writable, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mmap() error = %v, wantErr %v\n", err, tt.wantErr)
				return
			}
			if int64(len(got)) != tt.args.size {
				t.Errorf("Mmap() want buf size = %d, actual = %d", tt.args.size, len(got))
			}
		})
	}

}

func TestMunmap(t *testing.T) {
	fd, err := os.OpenFile("/tmp"+string(os.PathSeparator)+"mmap.txt", os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			err := os.Remove(fd.Name())
			assert.Nil(t, err)
		}
	}()

	buf, err := Mmap(fd, true, 100)
	assert.Nil(t, err)
	err = Munmap(buf)
	assert.Nil(t, err)
}
