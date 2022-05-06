package hash

import (
	"encoding/binary"
	"kv_engine/logfile"
)

type EncodeKey func(key, subKey []byte) []byte

type (
	// Hash hash table struct.
	Hash struct {
		record Record
	}

	// Record hash records to save.
	Record map[string]map[string][]byte
)

func New() *Hash {
	return &Hash{
		record: make(Record),
	}
}

// HSet Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
func (h *Hash) HSet(key, field string, value []byte) (res int) {
	// db 中不存在该key
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	// db 中存在要存储的hash 数据的field(key)
	if h.record[key][field] != nil {
		// if this field exists, overwritten it.
		h.record[key][field] = value
	} else {
		// create if this field not exists.
		h.record[key][field] = value
		res = 1
	}
	return
}

// HMSet same with above, but can set many fields
//   - HSet("myhash", "key1", "value1", "key2", "value2")
//   - HSet("myhash", []string{"key1", "value1", "key2", "value2"})
//   - HSet("myhash", map[string]interface{}{"key1": "value1", "key2": "value2"})
// need to come true
func (h *Hash) HMSet(key string, values ...any) (vals [][]byte) {
	return
}

// HSetNx sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
// Return if the operation successful
func (h *Hash) HSetNx(key, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	if _, ok := h.record[key][field]; !ok {
		// 不存在field
		h.record[key][field] = value
		res = 1
	}
	return res
}

// HGet returns the value associated with field in the hash stored at key.
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

// HGetAll returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
func (h *Hash) HGetAll(key string) [][]byte {
	if !h.exist(key) {
		return nil
	}
	res := make([][]byte, 0)
	for k, v := range h.record[key] {
		// 记录 field， v pair 对的样式
		res = append(res, []byte(k), v)
	}
	return res
}

// HDel removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (h *Hash) HDel(key, field string) bool {
	if !h.exist(key) {
		return false
	}

	if _, ok := h.record[key][field]; ok {
		delete(h.record[key], field)
		return true
	}
	return false
}

// HKeyExists returns if key exists in hash.
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

// HExists returns if field is an existing field in the hash stored at key.
func (h *Hash) HExists(key, field string) bool {
	if !h.exist(key) {
		return false
	}

	if _, ok := h.record[key][field]; ok {
		return true
	}
	return false
}

// HLen returns the number of fields contained in the hash stored at key.
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

// HKeys returns all field names in the hash stored at key.
func (h *Hash) HKeys(key string) []string {
	if !h.exist(key) {
		return nil
	}

	res := make([]string, 0)

	for k := range h.record[key] {
		res = append(res, k)
	}

	return res
}

// HVals returns all values in the hash stored at key.
func (h *Hash) HVals(key string) [][]byte {
	if !h.exist(key) {
		return nil
	}

	val := make([][]byte, 0)
	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return val
}

// HClear clear the key in hash.
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

// HStrLen Returns the string length of the value associated with field in the hash stored at key.
// If the key or the field do not exist, 0 is returned.
func (h *Hash) HStrLen(key, field string) int {
	if !h.exist(key) {
		return 0
	}

	return len(h.record[key][field])
}

// HMGet Returns the values associated with the specified fields in the hash stored at key.
// For every field that does not exist in the hash, a nil value is returned.
// Because non-existing keys are treated as empty hashes, running HMGET against a non-existing key will return a list of nil values.
func (h *Hash) HMGet(key string, fields ...string) (vals [][]byte) {
	length := len(fields)

	// key not exist
	if !h.exist(key) {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
		return vals
	}

	// key exist
	for _, field := range fields {
		val, ok := h.record[key][field]
		if !ok {
			vals = append(vals, nil)
		} else {
			vals = append(vals, val)
		}
	}
	return vals
}

// HIncreBy Increments the number stored at field in the hash stored at key by increment.
// If key does not exist, a new key holding a hash is created.
// If field does not exist the value is set to 0 before the operation is performed.
func (h *Hash) HIncrBy(key, field string, val int64) int {
	buf := make([]byte, binary.MaxVarintLen64)
	index := binary.PutVarint(buf, val)
	value := make([]byte, index)
	copy(value, buf[:index])
	if !h.exist(key) {
		h.HSet(key, field, value)
		return int(val)
	}

	if got, ok := h.record[key][field]; !ok {
		h.HSet(key, field, value)
		return int(val)
	} else {
		v, _ := binary.Varint(got)
		return int(val + v)
	}
}

// map whether exist key
func (h *Hash) exist(key string) bool {
	_, ok := h.record[key]
	return ok
}

func (h *Hash) IterateAndSend(channel chan *logfile.LogEntry, encode EncodeKey) {
	for key, h := range h.record {
		hashKey := []byte(key)
		for field, v := range h {
			encKey := encode(hashKey, []byte(field))
			channel <- &logfile.LogEntry{Key: encKey, Value: v}
		}
	}
}
