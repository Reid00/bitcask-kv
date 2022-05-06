package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var key = "my_hash"

func InitHash() *Hash {
	hash := New()

	hash.HSet(key, "a", []byte("hash_data_01"))
	hash.HSet(key, "b", []byte("hash_data_02"))
	hash.HSet(key, "c", []byte("hash_data_03"))
	hash.HSet(key, "d", []byte("hash_data_04"))
	return hash
}

func TestNew(t *testing.T) {
	hash := New()
	// assert.NotEqual(t, hash, nil)
	assert.NotNil(t, hash)
}

func TestHash_HSet(t *testing.T) {
	hash := InitHash()

	r1 := hash.HSet(key, "e", []byte("123"))
	assert.Equal(t, 1, r1)

	r2 := hash.HSet(key, "e", []byte("123"))
	assert.Equal(t, r2, 0)

	r3 := hash.HSet(key, "f", []byte("234"))
	assert.Equal(t, 1, r3)
}

func TestHSetNx(t *testing.T) {
	hash := InitHash()
	r1 := hash.HSetNx(key, "a", []byte("123"))
	assert.Equal(t, r1, 0)

	r1 = hash.HSetNx(key, "f", []byte("new field"))
	assert.Equal(t, r1, 1)

	r1 = hash.HSetNx(key, "f", []byte("new field"))
	assert.Equal(t, r1, 0)
}

func TestHGet(t *testing.T) {
	hash := InitHash()

	r1 := hash.HGet(key, "a")
	assert.Equal(t, []byte("hash_data_01"), r1)

	valNotExist := hash.HGet(key, "g")
	// nil 不能直接跟[]byte 比较，需要类型转化
	// assert.Equal(t, []byte(nil), valNotExist)
	assert.Nil(t, valNotExist)
}

func TestHGetAll(t *testing.T) {
	hash := InitHash()

	vals := hash.HGetAll(key)
	assert.Equal(t, 8, len(vals))
}

func TestHDel(t *testing.T) {
	hash := InitHash()

	v := hash.HDel(key, "a")
	assert.Equal(t, true, v)

	v = hash.HDel(key, "a")
	assert.Equal(t, false, v)

	v = hash.HDel(key, "g")
	assert.Equal(t, false, v)
}

func TestHExists(t *testing.T) {
	hash := InitHash()
	// key and field both exist
	exist := hash.HExists(key, "a")
	assert.Equal(t, true, exist)
	// key is non existing
	keyNot := hash.HExists("non exiting key", "a")
	assert.Equal(t, false, keyNot)
	not := hash.HExists(key, "m")
	assert.Equal(t, false, not)
}

func TestHKeys(t *testing.T) {
	hash := InitHash()

	keys := hash.HKeys(key)
	// assert.Equal(t, 4, len(keys))
	assert.Len(t, keys, 4)
	expected := []string{"a", "b", "c", "d"}
	// 切片用assert.Equal 有时候顺序不一致, 出错
	// assert.Equal(t, expected, keys)
	assert.ElementsMatch(t, keys, expected, "slice is not the same")

	keys = hash.HKeys("not-exist")

	// assert.Nil(t, res)
	assert.Equal(t, 0, len(keys))
	assert.ElementsMatch(t, keys, nil)
}

func TestHvals(t *testing.T) {
	hash := InitHash()

	values := hash.HVals(key)

	for _, v := range values {
		assert.NotNil(t, v)
	}
}

func TestHLen(t *testing.T) {
	hash := InitHash()
	assert.Equal(t, 4, hash.HLen(key))
}

func TestHClear(t *testing.T) {
	hash := InitHash()

	hash.HClear(key)
	v := hash.HGet(key, "a")
	assert.Nil(t, v)
}

func TestHkeyExists(t *testing.T) {
	hash := InitHash()

	ok := hash.HKeyExists(key)
	assert.Equal(t, true, ok)

	hash.HClear(key)
	ok = hash.HKeyExists(key)
	assert.Equal(t, false, ok)
}

func TestHMGet(t *testing.T) {
	hash := InitHash()

	type args struct {
		key   string
		field []string
	}

	tests := []struct {
		name    string
		args    args
		wantLen int
		want    [][]byte
	}{
		{
			"nil", args{key: key, field: nil}, 0, nil,
		},
		{
			"not-exist-key", args{key: "not-exist", field: []string{"a", "b"}}, 2, [][]byte{nil, nil},
		},
		{
			"not-exist-field", args{key: key, field: []string{"e"}}, 1, [][]byte{nil},
		},
		{
			"nomal", args{key: key, field: []string{"a", "b", "c"}}, 3, [][]byte{[]byte("hash_data_01"), []byte("hash_data_02"), []byte("hash_data_03")},
		},
		{
			"normal-2", args{key: key, field: []string{"a", "e", "c"}}, 3, [][]byte{[]byte("hash_data_01"), nil, []byte("hash_data_03")},
		},
	}
	// test 1 field get
	val := hash.HMGet(key, "a")
	assert.Equal(t, [][]byte{[]byte("hash_data_01")}, val)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vals := hash.HMGet(tt.args.key, tt.args.field...)
			assert.Equal(t, tt.wantLen, len(vals), "the result len is not the same!")
			assert.Equal(t, tt.want, vals, "the result is not the same!")
		})
	}

}
