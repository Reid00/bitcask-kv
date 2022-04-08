package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathExist(t *testing.T) {

	path1, err := filepath.Abs(filepath.Join("/tmp", "path", "kvdb-1"))
	assert.Nil(t, err)
	path2, err := filepath.Abs(filepath.Join("/tmp", "path", "kvdb-2"))
	assert.Nil(t, err)

	// 测试目录相关
	err = os.MkdirAll(path1, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		err := os.RemoveAll(filepath.Join("/tmp", "path"))
		t.Log(err)
	}()

	// 测试文件
	existedFile, err := filepath.Abs(filepath.Join("/tmp", "path", "kvdb-1.txt"))
	assert.Nil(t, err)
	notExistedFile, err := filepath.Abs(filepath.Join("/tmp", "path", "kbdb-2.txt"))
	assert.Nil(t, err)

	f, err := os.OpenFile(existedFile, os.O_CREATE, 0644)
	defer f.Close()
	assert.Nil(t, err)

	type args struct {
		path string
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"path exist", args{path: path1}, true,
		},
		{
			"path not exist", args{path: path2}, false,
		},
		{
			"file exists", args{path: existedFile}, true,
		},
		{
			"file not exists", args{path: notExistedFile}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathExist(tt.args.path); got != tt.want {
				t.Errorf("PathExist() = %v, want %v", got, tt.want)
			}
		})
	}

}

func TestCopyDir(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "test-copy-path"))
	assert.Nil(t, err)
	path2, err := filepath.Abs(filepath.Join("/tmp", "test-copy-paht-dest"))
	assert.Nil(t, err)

	subpath1 := filepath.Join(path, "sub1") // be the same result with below
	subpath2 := path + string(os.PathSeparator) + "sub2"
	subfile := path + string(os.PathSeparator) + "subfile.txt"

	err = os.MkdirAll(subpath1, os.ModePerm)
	assert.Nil(t, err)
	err = os.MkdirAll(subpath2, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		err = os.RemoveAll(path)
		t.Log(err)
		err = os.RemoveAll(path2)
		t.Log(err)
	}()

	f, err := os.OpenFile(subfile, os.O_CREATE, 0644)
	defer f.Close()
	assert.Nil(t, err)

	err = CopyDir(path, path2)
	assert.Nil(t, err)

}

func TestCopyFile(t *testing.T) {
	path := "/tmp/path/test"
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(path)

	file := "/tmp/path/test/001.log"
	f, err := os.OpenFile(file, os.O_CREATE, os.ModePerm)
	assert.Nil(t, err)
	defer f.Close()

	err = CopyFile(file, path+"/001.log.bak")
	assert.Nil(t, err)

}
