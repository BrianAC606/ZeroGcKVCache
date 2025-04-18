package kvcache

type Timer interface {
	Now() uint32
}