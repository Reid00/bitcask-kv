package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

// MaxHeaderSize max entry header size.
// crc32	typ    kSize	vSize	expiredAt
//  4    +   1   +   5   +   5    +    10      = 25 (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)

const MaxHeaderSize = 25

type EntryType byte

const (
	// TypeDelete represents entry type is delete.
	TypeDelete EntryType = iota + 1

	// TypeListMeta represents entry is list meta.
	TypeListMeta
)

type LogEntry struct {
	Key      []byte
	Value    []byte
	ExpireAt int64 // time.Unix
	Type     EntryType
}

type entryHeader struct {
	crc32     uint32 //check sum 校验和
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64
}

// EncodeEntry will encode entry into a byte slice.
// The encoded Entry looks like:
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//         |--------------------------crc check---------------------------|

// 编码entry 为字节序，并返回长度
func EncodeEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}

	header := make([]byte, MaxHeaderSize)
	// encoder header
	header[4] = byte(e.Type)
	var index = 5

	index += binary.PutVarint(header[index:], int64(len(e.Key)))   //kSize 写入字节序
	index += binary.PutVarint(header[index:], int64(len(e.Value))) // vSize 写入字节序
	index += binary.PutVarint(header[index:], e.ExpireAt)

	var size = index + len(e.Key) + len(e.Value)
	// copy encoded entry slice to buf slice
	buf := make([]byte, size)
	// header
	copy(buf[:index], header)
	// key
	copy(buf[index:], e.Key)
	// value
	copy(buf[index+len(e.Key):], e.Value)

	// crc32
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

// 解析entry 的header 和实际占用的字节长度.
// entryheader 最大25个字节，使用putVarint编码，较小数字不会占满所有字
func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 { // 只有crc 没有具体data
		return nil, 0
	}
	var entry entryHeader
	// entry crc
	entry.crc32 = binary.LittleEndian.Uint32(buf[:4])
	// entry type
	typ := buf[4]
	entry.typ = EntryType(typ)

	index := 5
	// entry kSize
	// varint 从buf 中解析int64, 并返回读取的字节数n
	ksize, n := binary.Varint(buf[index:])
	entry.kSize = uint32(ksize)
	index += n

	vsize, n := binary.Varint(buf[index:])
	entry.vSize = uint32(vsize)
	index += n

	expiredAt, n := binary.Varint(buf[index:])
	entry.expiredAt = int64(expiredAt)
	index += n

	return &entry, int64(index)
}

// 获取logEntry crc
// h: entry header 除去crc32的四个字节
func getEntryCrc(e *LogEntry, h []byte) uint32 {

	if e == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(h[:])

	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
