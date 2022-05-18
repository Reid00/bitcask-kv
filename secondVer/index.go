package kv_engine

import (
	"kv_engine/ds/art"
	"kv_engine/logfile"
	"kv_engine/logger"
	"time"
)

// DataType Define the data structure type.
type DataType = int8

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *RoseDB) buildIndex(dataType DataType, entry *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(entry, pos)
	case List:
		db.buildListIndex(entry, pos)
	case Hash:
		db.buildHashIndex(entry, pos)
	case Set:
		db.buildSetsIndex(entry, pos)
	case ZSet:
		db.buildZSetIndex(entry, pos)
	}
}

func (db *RoseDB) buildStrsIndex(entry *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()

	// 删除类型的Entry 或者已经过期
	if entry.Type == logfile.TypeDelete || (entry.ExpireAt != 0 && entry.ExpireAt < ts) {
		db.strIndex.idxTree.Delete(entry.Key)
		return
	}

	_, size := logfile.EncodeEntry(entry)
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}

	if db.opts.IndexMode == keyValueMemMode {
		idxNode.value = entry.Value
	}
	// 实际下面不做判断也可以，直接赋值， 如果为entry.ExpireAt == 0,
	// 给int64 零值， 也一样为0
	if entry.ExpireAt != 0 {
		idxNode.expiredAt = entry.ExpireAt
	}
	db.strIndex.idxTree.Put(entry.Key, idxNode)
}

func (db *RoseDB) buildListIndex(entry *logfile.LogEntry, pos *valuePos) {
	var listKey = entry.Key
	if entry.Type != logfile.TypeListMeta {
		listKey, _ = db.decodeListKey(entry.Key)
	}
}
func (db *RoseDB) buildHashIndex(entry *logfile.LogEntry, pos *valuePos) {

}

func (db *RoseDB) buildSetsIndex(entry *logfile.LogEntry, pos *valuePos) {

}
func (db *RoseDB) buildZSetIndex(entry *logfile.LogEntry, pos *valuePos) {

}

// getVal Get index info from a skip list in memory.
func (db *RoseDB) getVal(key []byte, dataType DataType) ([]byte, error) {
	var idxTree *art.AdaptiveRadixTree
	switch dataType {
	case String:
		idxTree = db.strIndex.idxTree
	case List:
		idxTree = db.listIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case Set:
		idxTree = db.setIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}

	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}

	idxNode, _ := rawValue.(*indexNode)
	if idxNode != nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()

	// key 过期
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, ErrKeyNotFound
	}

	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.opts.IndexMode == keyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// In KeyOnlyMemMode, the value not in memory, so get the value from log file at the offset.
	logFile := db.getActiveLogFile(dataType)
	if logFile.Fid != idxNode.fid {
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}

	entry, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}

	// key exists, but is invalid(deleted or expired)
	if entry.Type == logfile.TypeDelete || (entry.ExpireAt != 0 && entry.ExpireAt <= ts) {
		return nil, ErrKeyNotFound
	}
	return entry.Value, nil
}

// updateIndexTree 更新entry 这个entry 在IndexTree中的位置
func (db *RoseDB) updateIndexTree(ent *logfile.LogEntry, pos *valuePos, sendDiscard bool, dType DataType) error {
	var size = pos.entrySize

	if dType == String || dType == List {
		_, size = logfile.EncodeEntry(ent)
	}

	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == keyValueMemMode {
		idxNode.value = ent.Value
	}

	if ent.ExpireAt != 0 {
		idxNode.expiredAt = ent.ExpireAt
	}

	var idxTree *art.AdaptiveRadixTree
	switch dType {
	case String:
		idxTree = db.strIndex.idxTree
	case List:
		idxTree = db.listIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case Set:
		idxTree = db.setIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}

	oldVal, updated := idxTree.Put(ent.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated, dType)
	}
	return nil
}

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool, dataType DataType) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discards[dataType].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
}
