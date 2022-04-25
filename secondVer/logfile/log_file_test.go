package logfile

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenLogFile(t *testing.T) {
	t.Run("Fileio", func(t *testing.T) {
		testOpenLogFile(t, FileIO)
	})

	t.Run("Mmapio", func(t *testing.T) {
		testOpenLogFile(t, MMap)
	})
}

func testOpenLogFile(t *testing.T, ioType IOType) {
	type args struct {
		path   string
		fid    uint32
		fsize  int64
		ftype  FileType
		ioType IOType
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"zero-size", args{"/tmp", 0, 0, List, ioType}, true,
		},
		{
			"normal-size", args{"/tmp", 1, 100, List, ioType}, false,
		},
		{
			"big-size", args{"/tmp", 2, 1024 << 20, List, ioType}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLogF, err := OpenLogFile(tt.args.path, tt.args.fid, tt.args.fsize, tt.args.ftype, tt.args.ioType)
			defer func() {
				if gotLogF != nil && gotLogF.IoSelector != nil {
					_ = gotLogF.Delete()
				}
			}()

			if (err != nil) != tt.wantErr {
				t.Errorf("OpenLogFile() error=%v, wantErr = %v", err, tt.wantErr)
			}

			if !tt.wantErr && gotLogF == nil {
				t.Error("OpenFileLog() gotLogf == nil, but want not nil")
			}
		})
	}
}

func TestLogFile_Write(t *testing.T) {
	t.Run("Fileio", func(t *testing.T) {
		testLogFileWrite(t, FileIO)
	})

	t.Run("Mmapio", func(t *testing.T) {
		testLogFileWrite(t, MMap)
	})
}

func testLogFileWrite(t *testing.T, ioType IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, List, ioType)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			lf.Delete()
		}
	}()

	type fields struct {
		lf *LogFile
	}

	type args struct {
		buf []byte
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"nil", fields{lf: lf}, args{buf: nil}, false,
		},
		{
			// 空buf []byte{} byte类型的切片
			// 或者 空字符串转[]byte == []byte("")
			"no-value", fields{lf: lf}, args{buf: []byte{}}, false,
		},
		{
			"normal-1", fields{lf: lf}, args{buf: []byte("reidsdb")}, false,
		},
		{
			"normal-2", fields{lf: lf}, args{buf: []byte("data normal-2")}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fields.lf.Write(tt.args.buf); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogFile_Read(t *testing.T) {
	t.Run("FileIO", func(t *testing.T) {
		testLogFileRead(t, FileIO)
	})

	t.Run("Mmapio", func(t *testing.T) {
		testLogFileRead(t, MMap)
	})
}

func testLogFileRead(t *testing.T, ioType IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, List, ioType)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			lf.Delete()
		}
	}()

	data := [][]byte{
		[]byte("0"),
		[]byte("data sample1"),
		[]byte("data sample2"),
		[]byte("data sample3"),
		[]byte("data sample4"),
		[]byte("reidsdb"),
	}

	offset := writeSomeData(lf, data)

	type fields struct {
		lf *LogFile
	}

	type args struct {
		offset int64
		size   uint32
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"read-0", fields{lf: lf}, args{offset: offset[0], size: uint32(len(data[0]))}, data[0], false,
		},
		{
			"read-1", fields{lf: lf}, args{offset: offset[1], size: uint32(len(data[1]))}, data[1], false,
		},
		{
			"read-2", fields{lf: lf}, args{offset: offset[2], size: uint32(len(data[2]))}, data[2], false,
		},
		{
			"read-3", fields{lf: lf}, args{offset: offset[3], size: uint32(len(data[3]))}, data[3], false,
		},
		{
			"read-4", fields{lf: lf}, args{offset: offset[4], size: uint32(len(data[4]))}, data[4], false,
		},
		{
			"read-5", fields{lf: lf}, args{offset: offset[5], size: uint32(len(data[5]))}, data[5], false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.lf.Read(tt.args.offset, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("LogFile.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LogFile.Read() got = %v, want %v", got, tt.want)
			}
		})
	}

}

func writeSomeData(lf *LogFile, data [][]byte) []int64 {
	var offset []int64

	for _, v := range data {
		off := atomic.LoadInt64(&lf.WriteAt)
		offset = append(offset, off)
		if err := lf.Write(v); err != nil {
			panic(fmt.Sprintf("write data err: %+v", err))
		}
	}
	return offset
}

func TestLogFileReadEntry(t *testing.T) {
	t.Run("FileIO", func(t *testing.T) {
		testLogFileReadEntry(t, FileIO)
	})

	t.Run("MmapIO", func(t *testing.T) {
		testLogFileReadEntry(t, MMap)
	})
}

func testLogFileReadEntry(t *testing.T, ioType IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, Sets, ioType)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			lf.Delete()
		}
	}()
	// write some entries
	entries := []*LogEntry{
		{ExpireAt: 123332, Type: 0},
		{ExpireAt: 123332, Type: TypeDelete},
		{Key: []byte(""), Value: []byte(""), ExpireAt: 994332343, Type: TypeDelete},
		{Key: []byte("k1"), Value: nil, ExpireAt: 7844332343},
		{Key: nil, Value: []byte("reidsdb"), ExpireAt: 99400542343},
		{Key: []byte("k2"), Value: []byte("reidsdb"), ExpireAt: 8847333912},
		{Key: []byte("k3"), Value: []byte("some data"), ExpireAt: 8847333912, Type: TypeDelete},
	}

	var vals [][]byte

	// 准备log写入的数据，因为要读取Entry 所以要把entry 序列化之后写入log 中
	for _, e := range entries {
		v, _ := EncodeEntry(e)
		vals = append(vals, v)
	}

	offsets := writeSomeData(lf, vals)

	type fields struct {
		lf *LogFile
	}

	type args struct {
		offset int64
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *LogEntry
		want1   int64
		wantErr bool
	}{
		{
			"read-entry-0", fields{lf: lf}, args{offset: offsets[0]}, entries[0], int64(len(vals[0])), false,
		},
		{
			"read-entry-1", fields{lf: lf}, args{offset: offsets[1]}, entries[1], int64(len(vals[1])), false,
		},
		{
			// want nil slice
			// "read-entry-2", fields{lf: lf}, args{offset: offsets[2]}, entries[2], int64(len(vals[2])), false,
			"read-entry-2", fields{lf: lf}, args{offset: offsets[2]}, &LogEntry{ExpireAt: 994332343, Type: TypeDelete}, int64(len(vals[2])), false,
		},
		{
			// want value emtpy slice
			// "read-entry-3", fields{lf: lf}, args{offset: offsets[3]}, entries[3], int64(len(vals[3])), false,
			"read-entry-3", fields{lf: lf}, args{offset: offsets[3]}, &LogEntry{Key: []byte("k1"), Value: []byte{}, ExpireAt: 7844332343}, int64(len(vals[3])), false,
		},
		{
			// want key emtpy slice
			// "read-entry-4", fields{lf: lf}, args{offset: offsets[4]}, entries[4], int64(len(vals[4])), false,
			"read-entry-4", fields{lf: lf}, args{offset: offsets[4]}, &LogEntry{Key: []byte{}, Value: []byte("reidsdb"), ExpireAt: 99400542343}, int64(len(vals[4])), false,
		},
		{
			"read-entry-5", fields{lf: lf}, args{offset: offsets[5]}, entries[5], int64(len(vals[5])), false,
		},
		{
			"read-entry-6", fields{lf: lf}, args{offset: offsets[6]}, entries[6], int64(len(vals[6])), false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.fields.lf.ReadLogEntry(tt.args.offset)

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadLogEntry() got = %#v, want = %#v", got, tt.want)
			}

			if got1 != tt.want1 {
				t.Errorf("ReadLogEntry() got1 = %#v, want =%#v", got1, tt.want1)
			}
		})
	}

}

func TestLogFile_Sync(t *testing.T) {
	sync := func(ioType IOType) {
		file, err := OpenLogFile("/tmp", 0, 100, Hash, ioType)
		assert.Nil(t, err)
		defer func() {
			if file != nil {
				file.Delete()
			}
		}()

		err = file.Sync()
		assert.Nil(t, err)
	}

	t.Run("FileIO", func(t *testing.T) {
		sync(FileIO)
	})
	t.Run("MmapIO", func(t *testing.T) {
		sync(MMap)
	})

}

func TestLogFile_Close(t *testing.T) {
	var fid uint32 = 0

	closeLf := func(ioType IOType) {
		file, err := OpenLogFile("/tmp", fid, 100, Sets, ioType)
		assert.Nil(t, err)

		err = file.Close()
		assert.Nil(t, err)
	}

	t.Run("fileio", func(t *testing.T) {
		closeLf(FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		closeLf(MMap)
	})
}

func TestLogFile_Delete(t *testing.T) {
	deleteLf := func(ioType IOType) {
		file, err := OpenLogFile("/tmp", 0, 100, ZSet, ioType)
		assert.Nil(t, err)
		err = file.Delete()
		assert.Nil(t, err)
	}

	t.Run("fileio", func(t *testing.T) {
		deleteLf(FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		deleteLf(MMap)
	})
}
