package list

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList_LPush(t *testing.T) {
	lis := New()

	type args struct {
		key   []byte
		value []byte
	}

	tests := []struct {
		name string
		lis  *List
		args args
	}{
		{
			"nil", lis, args{key: nil, value: nil},
		},
		{
			"nil-value", lis, args{key: []byte("my_list"), value: nil},
		},
		{
			"normal", lis, args{key: []byte("my_list"), value: []byte("my_value")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.lis.LPush(tt.args.key, tt.args.value)
		})
	}
}

func TestList_RPush(t *testing.T) {

	lis := New()

	type args struct {
		key   []byte
		value []byte
	}

	tests := []struct {
		name string
		lis  *List
		args args
	}{
		{
			"nil", lis, args{key: nil, value: nil},
		},
		{
			"nil-value", lis, args{key: []byte("my_list"), value: nil},
		},
		{
			"normal", lis, args{key: []byte("my_list"), value: []byte("my_value")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.lis.RPush(tt.args.key, tt.args.value)
		})
	}
}

func TestList_LPop(t *testing.T) {
	lst := New()

	key := []byte("my_list")

	v := lst.LPop(key)
	assert.Nil(t, v)

	lst.LPush(key, []byte("v-000"))
	v = lst.LPop(key)
	assert.Equal(t, []byte("v-000"), v, "LPop result is not the same")

	v = lst.LPop(key)
	assert.Nil(t, v)

	lst.RPush(key, []byte("v-111"))
	v = lst.LPop(key)
	assert.Equal(t, []byte("v-111"), v)
}

func TestList_RPop(t *testing.T) {
	lst := New()

	key := []byte("my_list")

	v := lst.RPop(key)
	assert.Nil(t, v)

	lst.RPush(key, []byte("v-000"))
	v = lst.RPop(key)
	assert.Equal(t, []byte("v-000"), v, "LPop result is not the same")

	v = lst.RPop(key)
	assert.Nil(t, v)

	lst.LPush(key, []byte("v-111"))
	v = lst.RPop(key)
	assert.Equal(t, []byte("v-111"), v)
}

func TestList_LIndex(t *testing.T) {
	key := []byte("mylist")
	t.Run("lpush-index", func(t *testing.T) {
		lis := New()
		lis.LPush(key, []byte("l-val-1"))
		v1 := lis.LIndex(key, 1)
		assert.Nil(t, v1)

		v2 := lis.LIndex(key, 0)
		assert.Equal(t, v2, []byte("l-val-1"))

		v3 := lis.LIndex(key, -1)
		assert.Equal(t, v3, []byte("l-val-1"))
	})

	t.Run("rpush-index", func(t *testing.T) {
		lis := New()
		lis.RPush(key, []byte("r-val-1"))
		v1 := lis.LIndex(key, 1)
		assert.Nil(t, v1)

		v2 := lis.LIndex(key, 0)
		assert.Equal(t, v2, []byte("r-val-1"))

		v3 := lis.LIndex(key, -1)
		assert.Equal(t, v3, []byte("r-val-1"))
	})

	t.Run("multi-index", func(t *testing.T) {
		lis := New()
		lis.RPush(key, []byte("r-val-1"))
		lis.RPush(key, []byte("r-val-2"))
		lis.RPush(key, []byte("r-val-3"))
		lis.LPush(key, []byte("l-val-1"))
		lis.LPush(key, []byte("l-val-2"))
		lis.LPush(key, []byte("l-val-3"))

		v0 := lis.LIndex(key, 0)
		assert.Equal(t, v0, []byte("l-val-3"))

		v1 := lis.LIndex(key, -1)
		assert.Equal(t, v1, []byte("r-val-3"))

		v2 := lis.LIndex(key, 3)
		assert.Equal(t, v2, []byte("r-val-1"))
	})
}

func TestList_LSet(t *testing.T) {
	lis := New()
	key := []byte("mylist")
	lis.RPush(key, []byte("r-val-1"))
	lis.RPush(key, []byte("r-val-2"))
	lis.RPush(key, []byte("r-val-3"))
	lis.LPush(key, []byte("l-val-1"))
	lis.LPush(key, []byte("l-val-2"))
	lis.LPush(key, []byte("l-val-3"))

	type args struct {
		key   []byte
		index int
		value []byte
	}
	tests := []struct {
		name string
		lis  *List
		args args
		want bool
	}{
		{
			"zero", lis, args{key: key, index: 0, value: []byte("rosedb-1")}, true,
		},
		{
			"negative", lis, args{key: key, index: -2, value: []byte("rosedb-2")}, true,
		},
		{
			"normal", lis, args{key: key, index: 3, value: []byte("rosedb-3")}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lis.LSet(tt.args.key, tt.args.index, tt.args.value); got != tt.want {
				t.Errorf("LSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
