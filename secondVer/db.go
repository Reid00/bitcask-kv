package kv_engine

import (
	"errors"
	"kv_engine/logfile"
	"math"
)

var (
	// key not found
	ErrKeyNotFound = errors.New("Key not found")

	ErrLogFileNotFound = errors.New("Log file not found")

	ErrWrongNumberOfArgs = errors.New("Wrong number of arguments")

	ErrIntegerOverflow = errors.New("Increment or decrement overflow")

	ErrWrongValueType = errors.New("value is not an integer")

	ErrGCRunning = errors.New("log file gc is running, retry later")
)

const (
	logFileTypeNum   = 5
	encodeHeaderSize = 10
	initialListSeq   = math.MaxUint32 >> 1
	discardFilePath  = "DISCARD"
)

type (
	RoseDB struct {
		activeLogFile    map[DataType]*logfile.LogFile
		archivedLogFiles map[DataType]archivedFiles
		fidMap           map[DataType][]uint32 // only used at startup, never update even though log files changed.
		discards         map[DataType]*discard
	}

	archivedFiles map[uint32]*logfile.LogFile
)
