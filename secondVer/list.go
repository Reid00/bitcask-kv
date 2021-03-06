package kv_engine

import (
	"encoding/binary"

	"github.com/reid00/kv_engine/ds/art"
	"github.com/reid00/kv_engine/logfile"
	"github.com/reid00/kv_engine/logger"
)

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]

	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *RoseDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	db.listIndex.idxTree = db.listIndex.trees[string(key)]

	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first elements of the list stored at key.
func (db *RoseDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}

// RPop Removes and returns the last elements of the list stored at key.
func (db *RoseDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *RoseDB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return 0
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]

	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return 0
	}
	return int(tailSeq - headSeq - 1)

}

// LIndex returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
func (db *RoseDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]

	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return nil, err
	}

	var seq uint32
	// logical address to physical seq
	seq, err = db.convertLogicalIndexToSeq(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}

	// normalize seq
	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrIndexOutOfRange
	}

	encKey := db.encodeListKey(key, seq)

	val, err := db.getVal(encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil

}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element
// of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
// If start is larger than the end of the list, an empty list is returned.
// If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
func (db *RoseDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]
	// get List DataType meta info
	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return nil, err
	}

	var startSeq, endSeq uint32

	// logical address to physical address
	startSeq, err = db.convertLogicalIndexToSeq(headSeq, tailSeq, start)
	if err != nil {
		return nil, err
	}
	endSeq, err = db.convertLogicalIndexToSeq(headSeq, tailSeq, end)
	if err != nil {
		return nil, err
	}
	// normalize startSeq
	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}
	if startSeq >= tailSeq {
		return nil, ErrIndexOutOfRange
	}
	// normalize endSeq
	if endSeq >= tailSeq {
		endSeq = tailSeq - 1
	}
	if endSeq <= headSeq {
		return nil, ErrIndexOutOfRange
	}

	if startSeq > endSeq {
		return nil, ErrIndexStartLagerThanEnd
	}

	// the endSeq value is included
	for seq := startSeq; seq < endSeq+1; seq++ {
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(encKey, List)

		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// convertLogicalIndexToSeq just convert logical index to physical seq
// whether physical seq is legal or not, just convert
func (db *RoseDB) convertLogicalIndexToSeq(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32

	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}

func (db *RoseDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}

func (db *RoseDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}

func (db *RoseDB) listMeta(key []byte) (uint32, uint32, error) {
	val, err := db.getVal(key, List)
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

func (db *RoseDB) saveListMeta(key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)

	entry := &logfile.LogEntry{
		Key:   key,
		Value: buf,
		Type:  logfile.TypeListMeta,
	}

	pos, err := db.writeLogEntry(entry, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(entry, pos, true, List)
	return err
}

func (db *RoseDB) pushInternal(key, val []byte, isLeft bool) error {
	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return err
	}
	var seq = headSeq
	if !isLeft {
		seq = tailSeq
	}

	// List key ??????????????????????????????logEntry???
	encKey := db.encodeListKey(key, seq)

	ent := &logfile.LogEntry{Key: encKey, Value: val}

	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	if err = db.updateIndexTree(ent, valuePos, true, List); err != nil {
		return err
	}
	if isLeft {
		headSeq-- // ???list ??????index
	} else {
		tailSeq++
	}

	err = db.saveListMeta(key, headSeq, tailSeq)
	return err
}

func (db *RoseDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]

	headSeq, tailSeq, err := db.listMeta(key)
	if err != nil {
		return nil, err
	}

	size := tailSeq - headSeq - 1
	if size <= 0 {
		// reset meta
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveListMeta(key, headSeq, tailSeq)
		}
		return nil, nil
	}

	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}

	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(encKey, List)
	if err != nil {
		return nil, err
	}

	ent := &logfile.LogEntry{
		Key:  encKey,
		Type: logfile.TypeDelete,
	}

	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}

	oldVal, updated := db.listIndex.idxTree.Delete(encKey)
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.saveListMeta(key, headSeq, tailSeq); err != nil {
		return nil, err
	}

	// send discard
	db.sendDiscard(oldVal, updated, List)
	_, entrySize := logfile.EncodeEntry(ent)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return val, nil

}
