package main

import "encoding/binary"

type freelist struct {
	maxPage       pgnum
	releasedPages []pgnum
}

func newFreeList() *freelist {
	return &freelist{
		maxPage:       0,
		releasedPages: []pgnum{},
	}
}

func (fr *freelist) getNextPage() pgnum {
	if len(fr.releasedPages) != 0 {
		pageID := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pageID
	}
	fr.maxPage++
	return fr.maxPage
}

func (fr *freelist) releasePage(pageID pgnum) {
	fr.releasedPages = append(fr.releasedPages, pageID)
}

func (fr *freelist) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(fr.maxPage))
	pos += 2

	binary.LittleEndian.PutUint64(buf[pos:], uint64(len(fr.releasedPages)))
	pos += 2

	for _, page := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}

	return buf
}

func (fr *freelist) deserialize(buf []byte) {
	pos := 0
	fr.maxPage = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += 2

	releasedPagesCnt := int(binary.LittleEndian.Uint64(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPagesCnt; i++ {
		fr.releasedPages = append(fr.releasedPages, pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}
