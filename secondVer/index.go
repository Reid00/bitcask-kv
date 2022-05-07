package kv_engine

type DataType = int8

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)


