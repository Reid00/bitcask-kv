package kv_engine

import (
	"kv_engine/ioselector"
	"sync"
)

type discard struct {
	sync.Mutex
	valChan  chan *indexNode
	file     ioselector.IOSelector
	freeList []int64          // contains file offset that can be allocated
	location map[uint32]int64 // offset of each fid
}
