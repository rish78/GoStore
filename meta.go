package main

import "encoding/binary"

const (
	metaPageNum = 0
	pageNumSize = 8
)

type meta struct {
	freeListPage pgnum
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freeListPage))
	pos += pageNumSize
}

func (m *meta) deserialize(buf []byte) {
	pos := 0

	m.freeListPage = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
