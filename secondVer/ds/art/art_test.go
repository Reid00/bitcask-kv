package art

import (
	"reflect"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	tree := NewART()

	type args struct {
		key   []byte
		value any
	}

	tests := []struct {
		name        string
		art         *AdaptiveRadixTree
		args        args
		wantOldVal  any
		wantUpdated bool
	}{
		{
			"nil", tree, args{key: nil, value: nil}, nil, false,
		},
		{
			"normal-1", tree, args{key: []byte("1"), value: 11}, nil, false,
		},
		{
			"normal-2", tree, args{key: []byte("1"), value: 22}, 11, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldVal, updated := tree.Put(tt.args.key, tt.args.value)
			if !reflect.DeepEqual(oldVal, tt.wantOldVal) {
				t.Errorf("art tree.Put() oldVal: %v, wantVal: %v", oldVal, tt.wantOldVal)
			}
			if !reflect.DeepEqual(updated, tt.wantUpdated) {
				t.Errorf("art tree.Put() updated: %v, wantUpdated: %v", updated, tt.wantUpdated)
			}
		})
	}
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	tree := NewART()

	tree.Put(nil, nil)
	tree.Put([]byte("0"), 0)
	tree.Put([]byte("11"), 11)
	tree.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}

	tests := []struct {
		name string
		tree *AdaptiveRadixTree
		args args
		want any
	}{
		{"nil", tree, args{key: nil}, nil},
		{
			"zero", tree, args{key: []byte("0")}, 0,
		},
		{
			"rewrite-data", tree, args{key: []byte("11")}, "rewrite-data",
		},
		{
			"not-exist-data", tree, args{key: []byte("22")}, nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tree.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("art tree.Get() = %v, want %v", got, tt.want)
			}
		})
	}

}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	tree := NewART()

	tree.Put(nil, nil)
	tree.Put([]byte("0"), 0)
	tree.Put([]byte("11"), 11)
	tree.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}

	tests := []struct {
		name        string
		tree        *AdaptiveRadixTree
		args        args
		wantVal     any
		wantUpdated bool
	}{
		{
			"nil", tree, args{key: nil}, nil, false,
		},
		{
			"zero", tree, args{key: []byte("0")}, 0, true,
		},
		{
			"rewrite-data", tree, args{key: []byte("11")}, "rewrite-data", true,
		},
		{
			"not-exist", tree, args{key: []byte("22")}, nil, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, updated := tt.tree.Delete(tt.args.key)
			if !reflect.DeepEqual(val, tt.wantVal) {
				t.Errorf("art tree.Delete() got val: %v, want: %v", val, tt.wantVal)
			}

			if updated != tt.wantUpdated {
				t.Errorf("art tree.Delete() got updated: %v, want: %v", updated, tt.wantUpdated)
			}
		})

	}
}
