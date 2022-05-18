package kv_engine

import (
	"errors"
	"kv_engine/ds/art"
	"kv_engine/ds/zset"
	"kv_engine/flock"
	"kv_engine/ioselector"
	"kv_engine/logfile"
	"kv_engine/util"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	lockFileName     = "FLOCK"
)

type (
	RoseDB struct {
		activeLogFiles   map[DataType]*logfile.LogFile
		archivedLogFiles map[DataType]archivedFiles
		fidMap           map[DataType][]uint32 // only used at startup, never update even though log files changed.
		discards         map[DataType]*discard
		dumpState        ioselector.IOSelector
		opts             Options
		strIndex         *strIndex
		listIndex        *listIndex
		hashIndex        *hashIndex
		setIndex         *setIndex
		zsetIndex        *zsetIndex
		mu               *sync.RWMutex
		fileLock         *flock.FileLockGuard
		closed           uint32
		gcState          int32
	}

	archivedFiles map[uint32]*logfile.LogFile

	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}

	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	listIndex struct {
		mu      *sync.RWMutex
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu      *sync.RWMutex
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{
		idxTree: art.NewART(),
		mu:      new(sync.RWMutex),
	}
}

func newListIndex() *listIndex {
	return &listIndex{
		mu:    new(sync.RWMutex),
		trees: make(map[string]*art.AdaptiveRadixTree),
	}
}

func newHashIndex() *hashIndex {
	return &hashIndex{
		mu:    new(sync.RWMutex),
		trees: make(map[string]*art.AdaptiveRadixTree),
	}
}

func newSetIndex() *setIndex {
	return &setIndex{
		mu:      new(sync.RWMutex),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
	}
}

func newZSetIndex() *zsetIndex {
	return &zsetIndex{
		mu:      new(sync.RWMutex),
		indexes: zset.New(),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
	}
}

// Open a rosedb instance. You must call Close after using it.
func Open(opts Options) (*RoseDB, error) {
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// acquire file lock to prevent multiple processes from accessing the same directory.
	lockPath := filepath.Join(opts.DBPath, lockFileName)

	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[int8]archivedFiles),
		opts:             opts,
		fileLock:         lockGuard,
		strIndex:         newStrsIndex(),
		listIndex:        newListIndex(),
		hashIndex:        newHashIndex(),
		setIndex:         newSetIndex(),
		zsetIndex:        newZSetIndex(),
	}

	// init discard file
	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	// load the log files from disk
	if err := db.LoadLogFiles(); err != nil {
		return nil, err
	}

	// load indexes from log files
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// handle log files garbage collections
	go db.handleLogFileGC()
	return db, nil
}

func (db *RoseDB) initDiscard() error {
	discardPath := filepath.Join(db.opts.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	for i := String; i < logFileTypeNum; i++ {
		name := logfile.FileNamesMap[logfile.FileType(i)] + discardFileName
		dis, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = dis
	}

	db.discards = discards
	return nil
}

func (db *RoseDB) LoadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	dirEntrys, err := os.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}

	fidMap := make(map[DataType][]uint32)

	for _, file := range dirEntrys {
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(logfile.FileTypeMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}

	db.fidMap = fidMap

	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}

		if len(fids) == 0 {
			continue
		}

		// load log file in order
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		opts := db.opts

		for i, fid := range fids {
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
			lf, err := logfile.OpenLogFile(opts.DBPath, fid, opts.LogFileSizeThreshold, ftype, iotype)
			if err != nil {
				return err
			}

			// latest one is active log file
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = lf
			} else {
				db.archivedLogFiles[dataType][fid] = lf
			}
		}
	}
	return nil
}

func (db *RoseDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.archivedLogFiles[dataType] != nil {

	}

}

func (db *RoseDB) loadIndexFromLogFiles() {

}

// write entry to log file.
func (db *RoseDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}

	opts := db.opts
	entBuf, esize := logfile.EncodeEntry(ent)
	// activeLogFile 空间不足，需要新创建一个
	if activeLogFile.WriteAt+int64(esize) > opts.LogFileSizeThreshold {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}

		db.mu.Lock()
		// save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// open a new log file.
		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, ftype, iotype)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
		db.activeLogFiles[dataType] = lf
		activeLogFile = lf
		db.mu.Unlock()
	}

	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	// write entry and sync(if necessary)
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

func (db *RoseDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

func (db *RoseDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFiles[dataType] != nil {
		lf = db.archivedLogFiles[dataType][fid]
	}
	return lf
}
