package logfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/reid00/kv_engine/ioselector"
)

var (
	ErrInvalidCrc = errors.New("logfile: invalid crc32")

	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	ErrEndOfEntry = errors.New("logfile: end of entry in log file")

	// only support mmap and fileIO type
	ErrUnsupportedIoType = errors.New("unsupported io type")

	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	InitialLogFileId = 0

	FilePrefix = "log."
)

// 文件类型，有wal 和 value log
type FileType uint8

const (
	Strs FileType = iota
	List
	Hash
	Sets
	ZSet
)

var (
	FileNamesMap = map[FileType]string{
		Strs: "log.strs.",
		List: "log.list.",
		Hash: "log.hash.",
		Sets: "log.sets.",
		ZSet: "log.zset.",
	}

	// FileTypeMap name -> type
	FileTypeMap = map[string]FileType{
		"strs":  Strs,
		"list":  List,
		"hash":  Hash,
		"sets":  Sets,
		"zsets": ZSet,
	}
)

// represents different types of file io: FileIO(standard file io) and MMap(Memory Map).
type IOType uint8

const (
	// standard file io
	FileIO IOType = iota
	// Memery File
	MMap
)

// LogFile is an abstraction of a disk file, entry`s read and write will go through it.
type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64 // offset
	IoSelector ioselector.IOSelector
}

// 从LogFile 读取logEntry 在偏移量为offset处
// return 一个LogEntry, entry size 和一个error
// 如果offset 是无效的，返回的error 是IO.EOF
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// read LogEntry header
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, size := decodeHeader(headerBuf)
	// 读到了entry 的尾部
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &LogEntry{
		ExpireAt: header.expiredAt,
		Type:     header.typ,
	}

	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// 读取entry 的key 和 value
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}

		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}

	// crc32 check
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}

	return e, entrySize, nil
}

// 在offset 处，读取长度size
func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}

	buf := make([]byte, size)
	if _, err := lf.IoSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// LogFile 中，在writeAt 处写入数据buf[:].
// 注意写入时，要求计算writeat 时原子操作
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}

	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}

// LogFile 的指定位置处，读取长度为n的字节
func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

func (lf *LogFile) getLogFileName(path string, fid uint32, ftype FileType) (name string, err error) {
	if _, ok := FileNamesMap[ftype]; !ok {
		return "", ErrUnsupportedLogFileType
	}
	fname := FileNamesMap[ftype] + fmt.Sprintf("%09d", fid)
	name = filepath.Join(path, fname)
	return
}

// 打开一个已经存在的log 或者新建一个log 文件
// fsize 必须是>0, 根据ioType 创建ioselector 类型
func OpenLogFile(path string, fid uint32, fsize int64, ftype FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{
		Fid: fid,
	}
	fileName, err := lf.getLogFileName(path, fid, ftype)
	if err != nil {
		return nil, err
	}

	var selector ioselector.IOSelector

	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fsize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIoType
	}

	lf.IoSelector = selector
	return
}
