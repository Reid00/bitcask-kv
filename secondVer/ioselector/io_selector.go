package ioselector

import (
	"errors"
	"os"
)

var ErrInvalidFsize = errors.New("fsize can't be sero or negative")

const FilePerm = 0644

type IOSelector interface {
	// 往log file offset 出写入一个切片b
	Write(b []byte, offset int64) (int, error)

	// 从offset 处，读取数据到切片b中
	Read(b []byte, offset int64) (int, error)

	// 内存中的数据，刷盘到硬盘
	Sync() error
	// 关闭文件，将不可进行io
	Close() error
	// 删除文件
	Delete() error
}

// 打开文件，并且当文件大小小于fsize 的时候，截断文件为fsize 大小
// 保证文件的最大值相同
func openFile(fname string, fsize int64) (*os.File, error) {

	fd, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}
