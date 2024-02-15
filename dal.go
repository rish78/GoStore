package main

import (
	"errors"
	"fmt"
	"os"
)

type pgnum uint64

type Page struct {
	num  pgnum
	data []byte
}

type Dal struct {
	file     *os.File
	pageSize int

	*freelist
	*meta
}

func NewDal(path string) (*Dal, error) {
	dal := &Dal{
		meta:     newEmptyMeta(),
		pageSize: os.Getpagesize(),
	}

	// exist
	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		meta, err := dal.ReadMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.readFreeList()
		if err != nil {
			return nil, err
		}
		dal.freelist = freelist
		// doesn't exist
	} else if errors.Is(err, os.ErrNotExist) {
		// init freelist
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		dal.freelist = newFreeList()
		dal.freeListPage = dal.getNextPage()
		_, err := dal.writeFreeList()
		if err != nil {
			return nil, err
		}

		// write meta page
		_, err = dal.WriteMeta(dal.meta) // other error
	} else {
		return nil, err
	}
	return dal, nil
}

func (d *Dal) Close() error {
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return fmt.Errorf("could not close file: %v", err)
		}
		d.file = nil
	}
	return nil
}

func (d *Dal) AllocateEmptyPage() *Page {
	return &Page{
		data: make([]byte, os.Getpagesize()),
	}
}

func (d *Dal) ReadPage(pageNum pgnum) (*Page, error) {
	p := d.AllocateEmptyPage()

	offset := int(pageNum) * d.pageSize

	_, err := d.file.ReadAt(p.data, int64(offset))
	if err != nil {
		return nil, err
	}

	return p, err
}

func (d *Dal) WritePage(p *Page) error {
	offset := int(p.num) * d.pageSize
	_, err := d.file.WriteAt(p.data, int64(offset))
	return err
}

func (d *Dal) WriteMeta(meta *meta) (*Page, error) {
	p := d.AllocateEmptyPage()
	p.num = metaPageNum
	meta.serialize(p.data)
	if err := d.WritePage(p); err != nil {
		return nil, err
	}
	return p, nil
}

func (d *Dal) ReadMeta() (*meta, error) {
	p, err := d.ReadPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data)
	return meta, nil
}

func (d *Dal) readFreeList() (*freelist, error) {
	p, err := d.ReadPage(d.freeListPage)
	if err != nil {
		return nil, err
	}

	freelist := newFreeList()
	freelist.deserialize(p.data)
	return freelist, nil
}

func (d *Dal) writeFreeList() (*Page, error) {
	p := d.AllocateEmptyPage()
	p.num = d.freeListPage
	d.freelist.serialize(p.data)

	err := d.WritePage(p)
	if err != nil {
		return nil, err
	}
	d.freeListPage = p.num
	return p, nil
}

func (d *Dal) getNode(pageNum pgnum) (*Node, error) {
	p, err := d.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}

	node := NewEmptyNode()
	node.deserialize(p.data)
	node.pageNum = pageNum
	return node, nil
}

func (d *Dal) writeNode(n *Node) (*Node, error) {
	p := d.AllocateEmptyPage()
	if n.pageNum == 0 {
		p.num = n.getNextPage()
		n.pageNum = p.num
	} else {
		p.num = n.pageNum
	}

	p.data = n.serialize(p.data)

	err := d.WritePage(p)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (d *Dal) deleteNode(pageNum pgnum) {
	d.releasePage(pageNum)
}
