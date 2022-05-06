package list

import (
	"encoding/binary"
	"kv_engine/logfile"
	"kv_engine/logger"
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
