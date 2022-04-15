package logfile

import (
	"errors"
	"sync"
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
