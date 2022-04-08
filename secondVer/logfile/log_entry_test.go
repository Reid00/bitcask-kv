package logfile

import (
	"reflect"
	"testing"
)

func TestEncodeEntry(t *testing.T) {
	type args struct {
		e *LogEntry
	}

	tests := []struct {
		name  string
		args  args
		want  []byte
		want2 int
	}{
		{
			"nil entry", args{nil}, nil, 0,
		},
		{
			// entry 是零值，key,value 都是零值，0，只有crc32校验码有效，uint32 恒定4个字节
			// type, ksize, vsize, expireat 都是0，binary.PutVarint, 只用了1字节
			"no-fields", args{e: &LogEntry{}}, []byte{28, 223, 68, 33, 0, 0, 0, 0}, 8,
		},
		{
			"no-key-value", args{e: &LogEntry{ExpireAt: 1615972690}}, []byte{167, 25, 217, 62, 0, 0, 0, 164, 165, 142, 133, 12}, 12,
		},
		{
			"with-key-value", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("lotusdb"), ExpireAt: 1615972690}},
			[]byte{61, 215, 197, 153, 0, 4, 14, 164, 165, 142, 133, 12, 107, 118, 108, 111, 116, 117, 115, 100, 98}, 21,
		},
		{
			"type-delete", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("lotusdb"), ExpireAt: 1615972690, Type: TypeDelete}},
			[]byte{126, 28, 99, 30, 1, 4, 14, 164, 165, 142, 133, 12, 107, 118, 108, 111, 116, 117, 115, 100, 98}, 21,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2 := EncodeEntry(tt.args.e)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeEntry() got = %v, want %v", got, tt.want)
			}
			if got2 != tt.want2 {
				t.Errorf("EncodeEntry() got = %v, want %v", got2, tt.want2)

			}
		})
	}

}

func Test_decodeHeader(t *testing.T) {
	type args struct {
		buf []byte
	}

	tests := []struct {
		name  string
		args  args
		want  *entryHeader
		want2 int64
	}{
		{"nil", args{buf: nil}, nil, 0},
		{"no-enough-bytes", args{buf: []byte{1, 4, 3, 22}}, nil, 0},
		{
			"no-fields", args{buf: []byte{28, 223, 68, 33, 0, 0, 0, 0}}, &entryHeader{crc32: 558161692}, 8,
		},
		{
			// 可以是这个logEntry Key: []byte("kv"), Value: []byte("lotusdb"), ExpireAt: 1615972690
			"normal", args{buf: []byte{61, 215, 197, 153, 0, 4, 14, 164, 165, 142, 133, 12}}, &entryHeader{crc32: 2579879741, typ: 0, kSize: 2, vSize: 7, expiredAt: 1615972690}, 12,
		},
		{
			// 可以是这个logEntry Key: []byte("kv"), Value: []byte("lotusdb"), ExpireAt: 1615972690, type: TypeDelete
			"delete", args{buf: []byte{126, 28, 99, 30, 1, 4, 14, 164, 165, 142, 133, 12}}, &entryHeader{crc32: 509811838, typ: 1, kSize: 2, vSize: 7, expiredAt: 1615972690}, 12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2 := decodeHeader(tt.args.buf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeHeader() got = %v, want %v", got, tt.want)
			}
			if got2 != tt.want2 {
				t.Errorf("decodeHeader() got = %v, want %v", got2, tt.want2)
			}
		})
	}

}

func Test_getEntryCrc(t *testing.T) {
	type args struct {
		e *LogEntry
		h []byte
	}

	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			"nil", args{e: nil, h: nil}, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}, h: []byte{0, 0, 0, 0}}, 558161692,
		},
		{
			"normal", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("lotusdb")}, h: []byte{0, 4, 14, 198, 147, 242, 166, 3}}, 2631913573,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getEntryCrc(tt.args.e, tt.args.h)
			if got != tt.want {
				t.Errorf("getEntryCrc() = %v, want %v", got, tt.want)
			}
		})
	}

}
