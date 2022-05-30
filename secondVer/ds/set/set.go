package set

import (
	"github.com/reid00/kv_engine/logfile"
)

var existFlag = struct{}{}

type (
	// set index
	Set struct {
		record Record
	}

	// records in set to save.
	Record map[string]map[string]struct{}
)

// New a set instance
func New() *Set {
	return &Set{make(Record)}
}

// SAdd Add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
// TODO need optimization, add multi field, return number is actually added.
// FIXED
func (s *Set) SAdd(key string, members ...[]byte) int {
	// key not exist
	if !s.exist(key) {
		// init key firstly
		s.record[key] = make(map[string]struct{})
	}
	var count int
	for _, v := range members {
		if _, ok := s.record[key][string(v)]; !ok {
			s.record[key][string(v)] = existFlag
			count++
		}
	}
	return count
}

// SPop Removes and returns one or more random members from the set value store at key.
// This operation is similar to SRANDMEMBER, that returns one or more random elements from a set but does not remove it.
// By default, the command pops a single member from the set. When provided with the optional count argument,
// the reply will consist of up to count members, depending on the set's cardinality.
// NOTE in redis count is default args which is allow assign this arg so use SPop + SPopN
func (s *Set) SPopN(key string, count int) [][]byte {
	if !s.exist(key) || count <= 0 {
		return nil
	}

	vals := make([][]byte, 0, count)

	for k := range s.record[key] {
		delete(s.record[key], k)

		vals = append(vals, []byte(k))
		/* or
		len(vals) == count{
			break
		}
		*/
		count--
		if count == 0 {
			break
		}
	}
	return vals
}

func (s *Set) SPop(key string) []byte {
	if !s.exist(key) {
		return nil
	}

	var val []byte
	for k := range s.record[key] {
		val = []byte(k)
		delete(s.record[key], k)
		break
	}
	return val
}

// SIsMember Returns if member is a member of the set stored at key.
func (s *Set) SIsMember(key string, member []byte) int {
	if !s.exist(key) {
		return 0
	}

	if _, ok := s.record[key][string(member)]; !ok {
		return 0
	}
	return 1
}

// SRandMember When called with just the key argument, return a random element from the set value stored at key.
// NOTE the same with SPop need add SRandMember
func (s *Set) SRandMemberN(key string, count int) [][]byte {
	// similar with SPop
	if !s.exist(key) {
		return nil
	}
	if count == 0 {
		return [][]byte{} // empty slice not nil
	}

	vals := make([][]byte, 0)

	if count > 0 {
		for k := range s.record[key] {
			vals = append(vals, []byte(k))
			count--
			if count == 0 {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.record[key] {
				return []byte(k)
			}
			return nil
		}

		for count > 0 {
			vals = append(vals, randomVal())
			count--
		}
	}
	return vals
}

// SRem Remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
// Return the number of members that were removed from the set, not including non existing members.
func (s *Set) SRem(key string, memebers ...[]byte) int {
	if !s.exist(key) {
		return 0
	}

	var count int

	for _, v := range memebers {
		if _, ok := s.record[key][string(v)]; ok {
			delete(s.record[key], string(v))
			count++
		}
	}
	return count
}

// SMove Move member from the set at source to the set at destination.
// If the source set does not exist or does not contain the specified element,no operation is performed and returns 0.
func (s *Set) SMove(srcKey, dstKey string, member []byte) int {
	if !s.exist(srcKey) || !s.exist(dstKey) {
		return 0
	}

	if _, ok := s.record[srcKey][string(member)]; !ok {
		return 0
	}
	delete(s.record[srcKey], string(member))
	return s.SAdd(dstKey, member)
}

// SCard Returns the set cardinality (number of elements) of the set stored at key.
func (s *Set) SCard(key string) int {
	return len(s.record[key])
}

// SMembers Returns all the members of the set value stored at key.
func (s *Set) SMembers(key string) [][]byte {
	if !s.exist(key) {
		return nil
	}

	var vals [][]byte
	for k := range s.record[key] {
		vals = append(vals, []byte(k))
	}
	return vals
}

// SUnion Returns the members of the set resulting from the union of all the given sets.
func (s *Set) SUnion(keys ...string) (vals [][]byte) {
	m := make(map[string]struct{})

	for _, k := range keys {
		if s.exist(k) {
			// exist key, get set member
			for member := range s.record[k] {
				m[member] = struct{}{}
			}
		}
	}

	for member := range m {
		vals = append(vals, []byte(member))
	}
	return
}

// SDiff Returns the members of the set resulting from the difference between the first set and all the successive sets.
func (s *Set) SDiff(keys ...string) (vals [][]byte) {
	// keys[0] must exist diff keys[1:]...
	if len(keys) == 0 || !s.exist(keys[0]) {
		return
	}

	// BUG if flag here
	// var flag bool // 是否是其他key 的成员

	for k := range s.record[keys[0]] {
		// FIXED above
		var flag bool
		// 一一比较 k 是否是后面key的member
		for i := 1; i < len(keys); i++ {
			if _, ok := s.record[keys[i]][k]; ok { // k是其他key 的member
				flag = true
				break
			}
			// ok := s.SIsMember(keys[i], []byte(k))
			// if ok == 1 {
			// 	flag = true
			// 	break
			// }
		}
		if !flag {
			vals = append(vals, []byte(k))
		}
	}
	return
}

// SKeyExists returns if the key exists.
func (s *Set) SKeyExists(key string) bool {
	return s.exist(key)
}

// SClear clear the specified key in set.
func (s *Set) SClear(key string) bool {
	if !s.exist(key) {
		return true
	}
	delete(s.record, key)
	return true
}

func (s *Set) exist(key string) bool {
	// if _, ok := s.record[key]; !ok {
	// 	return false
	// }
	// return true

	_, ok := s.record[key]
	return ok
}

func (s *Set) IterateAndSend(chn chan *logfile.LogEntry) {
	for key, rec := range s.record {
		setKey := []byte(key)
		for member := range rec {
			chn <- &logfile.LogEntry{Key: setKey, Value: []byte(member)}
		}
	}
	return
}
