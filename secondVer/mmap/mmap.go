package mmap

import (
	"os"
)

// 内存映射方法，写入文件时，使用mmap syscall,减少两次IO
// 可以再用户空间直接读写page cache， 避免了，从 page cache -> use buffer -> socket buffer/page cahe
// 减少两次上下文切换

// Mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// Munmap unmaps a previously mapped slice.
func Munmap(b []byte) error {
	return munmap(b)
}

// Madvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. Set the readahead flag to
// false if page references are expected in random order.
func Madvise(b []byte, readahead bool) error {
	return madvise(b, readahead)
}

// Msync would call sync on the mmapped data.
func Msync(b []byte) error {
	return msync(b)
}
