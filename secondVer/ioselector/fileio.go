package ioselector

import "os"

type FileIOSelector struct {
	fd *os.File
}

func NewFileIOSelector(fname string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}

	file, err := openFile(fname, fsize)
	if err != nil {
		return nil, err
	}

	return &FileIOSelector{fd: file}, nil
}

// Write is a wrapper of os.File WriteAt
func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

// delete file descriptor if we don't use it anymore
func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}

	return os.Remove(fio.fd.Name())
}
