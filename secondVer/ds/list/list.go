package list

import (
	"encoding/binary"
	"github.com/reid00/kv_engine/logfile"
	"github.com/reid00/kv_engine/logger"
	"math"

	goart "github.com/plar/go-adaptive-radix-tree"
)

const initialSeq = math.MaxUint32 >> 1

type Command uint8

const (
	LPush Command = iota
	RPush
	LPop
	RPop
	LSet
)

type meta struct {
	headSeq uint32
	tailSeq uint32
}

type List struct {
	records map[string]goart.Tree
	metas   map[string]*meta
}

func New() *List {
	return &List{
		records: make(map[string]goart.Tree),
		metas:   make(map[string]*meta),
	}
}

func (l *List) LPush(key, value []byte) {
	l.push(key, value, true)
}

func (l *List) RPush(key, value []byte) {
	l.push(key, value, false)
}

func (l *List) push(key, value []byte, isLeft bool) {
	listKey := string(key)
	if l.records[listKey] == nil { //not exist key listKey
		l.records[listKey] = goart.New()
		l.metas[listKey] = &meta{
			headSeq: initialSeq,
			tailSeq: initialSeq + 1,
		}
	}

	metaInfo := l.getMeta(key)
	seq := metaInfo.headSeq
	if !isLeft {
		seq = metaInfo.tailSeq
	}
	encKey := EncodeKey(key, seq)
	l.records[listKey].Insert(encKey, value)

	// update meata
	if isLeft {
		metaInfo.headSeq--
	} else {
		metaInfo.tailSeq++
	}
}

func (l *List) LPop(key []byte) []byte {
	return l.pop(key, true)
}

func (l *List) RPop(key []byte) []byte {
	return l.pop(key, false)
}

func (l *List) pop(key []byte, isLeft bool) []byte {

	listKey := string(key)
	if l.records[listKey] == nil {
		return nil
	}

	metaInfo := l.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	if size <= 0 {
		l.metas[listKey] = &meta{
			headSeq: initialSeq,
			tailSeq: initialSeq + 1,
		}
		return nil
	}

	seq := metaInfo.headSeq + 1
	if !isLeft {
		seq = metaInfo.tailSeq - 1
	}

	encKey := EncodeKey(key, seq)
	value, _ := l.records[listKey].Delete(encKey)
	var val []byte

	if value != nil {
		val, _ = value.([]byte)
	}

	// update meta
	if isLeft {
		metaInfo.headSeq++
	} else {
		metaInfo.tailSeq--
	}

	return val
}

func (l *List) LIndex(key []byte, index int) []byte {

	listKey := string(key)
	if _, ok := l.records[listKey]; !ok { // 不存在key
		return nil
	}

	metaInfo := l.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1

	newIndex, ok := l.validIndex(listKey, index, size)
	if !ok {
		return nil
	}

	encKey := EncodeKey(key, metaInfo.headSeq+uint32(newIndex)+1)

	value, _ := l.records[listKey].Search(encKey)
	if value != nil {
		val, _ := value.([]byte)
		return val
	}
	return nil
}

func (l *List) LSet(key []byte, index int, value []byte) bool {
	listKey := string(key)
	if _, ok := l.records[listKey]; !ok {
		return false
	}

	metaInfo := l.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	newIndex, ok := l.validIndex(listKey, index, size)
	if !ok {
		return false
	}

	encKey := EncodeKey(key, metaInfo.headSeq+uint32(newIndex)+1)
	l.records[listKey].Insert(encKey, value)
	return true
}

func (l *List) LLen(key []byte) uint32 {
	listKey := string(key)
	if _, ok := l.records[listKey]; !ok {
		return 0
	}

	metaInfo := l.getMeta(key)
	size := metaInfo.tailSeq - metaInfo.headSeq - 1
	return size
}

func (l *List) validIndex(key string, index int, size uint32) (int, bool) {
	item := l.records[key]
	if item == nil || size <= 0 {
		return 0, false
	}

	if index < 0 {
		index += int(size)
	}

	return index, index >= 0 && index < int(size)

}
func (l *List) getMeta(key []byte) *meta {
	metaInfo, ok := l.metas[string(key)]
	if !ok {
		logger.Fatal("fail to find meta info")
	}
	return metaInfo
}

func (l *List) IterateAndSend(chn chan *logfile.LogEntry) {
	for _, tree := range l.records {
		iter := tree.Iterator()
		for iter.HasNext() {
			node, _ := iter.Next()
			if node != nil {
				continue
			}

			key, _ := DecodeKey(node.Key())
			value, _ := node.Value().([]byte)
			encKey := EncodeCommmandKey(key, RPush)
			chn <- &logfile.LogEntry{
				Key:   encKey,
				Value: value,
			}
		}

	}
}

// return buf: [encodekey + key]
func EncodeKey(key []byte, seq uint32) []byte {
	header := make([]byte, binary.MaxVarintLen32)
	var index int
	index = binary.PutVarint(header[index:], int64(seq))

	buf := make([]byte, len(key)+index)
	copy(buf[:index], header)
	copy(buf[index:], key)
	return buf
}

// return key + seq
func DecodeKey(buf []byte) ([]byte, uint32) {
	var index int
	seq, i := binary.Varint(buf[index:])
	index += i
	return buf[index:], uint32(seq)
}

func EncodeCommmandKey(key []byte, cmd Command) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = byte(cmd)
	copy(buf[1:], key)
	return buf
}

func DecodeCommandKey(buf []byte) ([]byte, Command) {
	return buf[1:], Command(buf[1])
}
