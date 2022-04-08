package minidb

import "os"

const FileName = "minidb.data"
const MergeFileName = "minidb.data.merge"

// DBFile 定义
type DBFile struct {
	File   *os.File //实际的存储文件
	Offset int64    // 文件的大小
}

func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DBFile{File: file, Offset: stat.Size()}, nil
}

// 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

// 新建一个合并时的数据文件
func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

// Read 从offset 处开始读取
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil { // 命名返回值变量, e, err 已经定义好了
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize

	if e.KeySize > 0 { // 存在key
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)

	if e.ValueSize > 0 { // 存在value
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// 写入Entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
