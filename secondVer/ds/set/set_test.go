package set

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var key = "my_set"

func InitSet() *Set {
	set := New()

	set.SAdd(key, []byte("a"))
	set.SAdd(key, []byte("b"))
	set.SAdd(key, []byte("c"))
	set.SAdd(key, []byte("d"))
	set.SAdd(key, []byte("e"))
	set.SAdd(key, []byte("f"))

	return set
}

func TestSet_SAdd(t *testing.T) {
	set := InitSet()

	n := set.SAdd(key, []byte("abcd"))
	assert.Equal(t, 1, n)

	n = set.SAdd(key, []byte("a"), []byte("good"), []byte("nice"))
	assert.Equal(t, 2, n)
}

func TestSet_SPop(t *testing.T) {
	set := InitSet()

	vals := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}

	val := set.SPop(key)
	// t.Log("delete: ", string(val))
	assert.Equal(t, 5, set.SCard(key), "remains length is not the same")

	val = set.SPop(key)
	// t.Log("delete: ", string(val))
	assert.Contains(t, vals, val, "val not this vals")
	assert.Equal(t, 4, set.SCard(key), "remains length is not the same")
}

func TestSet_SPopN(t *testing.T) {
	set := InitSet()

	expected := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}

	vals := set.SPopN(key, 2)
	assert.Equal(t, 4, set.SCard(key), "remains length is not the same")
	for _, v := range vals {
		// t.Log("poped v: ", string(v))
		assert.Contains(t, expected, v, "v not in expected")
	}
	// count > set.Scard()
	vals = set.SPopN(key, 7)
	assert.Equal(t, 4, len(vals), "return length is not expected")
	assert.Equal(t, 0, set.SCard(key), "remains length is not the same")
	for _, v := range vals {
		// t.Log("poped v: ", string(v))
		assert.Contains(t, expected, v, "v not in expected")
	}
}

func TestSet_SIsMember(t *testing.T) {
	set := InitSet()

	isMem := set.SIsMember(key, []byte("a"))
	assert.Equal(t, 1, isMem)
	isMem = set.SIsMember(key, []byte("g"))
	assert.Equal(t, 0, isMem)

	isMem = set.SIsMember("not-exist-key", []byte("g"))
	assert.Equal(t, 0, isMem)

}

func TestSet_SRandMemberN(t *testing.T) {
	set := InitSet()

	m := set.SRandMemberN(key, 0)
	assert.Equal(t, [][]byte{}, m)

	m = set.SRandMemberN("not-exist-key", 1)
	assert.Nil(t, m)
	assert.Equal(t, [][]byte(nil), m)

	m = set.SRandMemberN(key, 2)
	assert.Equal(t, 2, len(m))

	// total scard 6
	m = set.SRandMemberN(key, 7)
	assert.Equal(t, 6, len(m))

	m = set.SRandMemberN(key, -7)
	assert.Equal(t, 7, len(m))
}

func TestSet_SRem(t *testing.T) {
	set := InitSet()

	n := set.SRem("not-exist-key", []byte("a"))
	assert.Equal(t, 0, n)

	n = set.SRem(key, []byte("a"))
	assert.Equal(t, 1, n)

	n = set.SRem(key, []byte("b"), []byte("g"))
	assert.Equal(t, 1, n)

	n = set.SRem(key, [][]byte{[]byte("c"), []byte("g")}...)
	assert.Equal(t, 1, n)
}

func TestSet_SMove(t *testing.T) {
	set := InitSet()

	set.SAdd("set2", []byte("good"))

	move := set.SMove(key, "set2", []byte("a"))
	assert.Equal(t, 1, move)
	move = set.SMove(key, "set2", []byte("f"))
	assert.Equal(t, 1, move)
	move = set.SMove(key, "set2", []byte("12332"))
	assert.Equal(t, 0, move)
}

func TestSet_SCard(t *testing.T) {
	set := InitSet()
	card := set.SCard(key)
	assert.Equal(t, 6, card)
}

func TestSet_SMembers(t *testing.T) {
	set := InitSet()
	members := set.SMembers(key)
	assert.Equal(t, 6, len(members))
	expected := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}
	assert.ElementsMatch(t, expected, members, "SMembers() not expected.")
}

func TestSet_SUnion(t *testing.T) {
	set := InitSet()

	set.SAdd("set2", []byte("h"))
	set.SAdd("set2", []byte("f"))
	set.SAdd("set2", []byte("g"))
	members := set.SUnion(key, "set2")
	assert.Equal(t, 8, len(members))
}

func TestSet_SDiff(t *testing.T) {
	set := InitSet()
	set.SAdd("set2", []byte("a"))
	set.SAdd("set2", []byte("f"))
	set.SAdd("set2", []byte("g"))

	set.SAdd("set3", []byte("b"))

	t.Run("normal situation", func(t *testing.T) {
		members := set.SDiff(key, "set2")
		assert.Equal(t, 4, len(members))
	})

	t.Run("one key", func(t *testing.T) {
		members := set.SDiff(key)
		assert.Equal(t, 6, len(members))
	})

	t.Run("three key", func(t *testing.T) {
		members := set.SDiff(key, "set2", "set3")
		assert.Equal(t, 3, len(members))
	})

	t.Run("first key not exists", func(t *testing.T) {
		members := set.SDiff("not-exist", key)
		assert.Equal(t, 0, len(members))
	})

	t.Run("empty key", func(t *testing.T) {
		var keySet []string
		members := set.SDiff(keySet...)
		assert.Nil(t, members)
	})
}

func TestSet_SClear(t *testing.T) {
	set := InitSet()
	set.SClear(key)

	val := set.SMembers(key)
	assert.Equal(t, len(val), 0)
}

func TestSet_SKeyExists(t *testing.T) {
	set := InitSet()

	exists1 := set.SKeyExists(key)
	assert.Equal(t, exists1, true)

	set.SClear(key)

	exists2 := set.SKeyExists(key)
	assert.Equal(t, exists2, false)
}

func TestNew(t *testing.T) {
	set := New()
	assert.NotEqual(t, set, nil)
}
