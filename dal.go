package main

import (
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
}

func NewDal(path string, pageSize int) (*Dal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dal := &Dal{
		file:     file,
		pageSize: pageSize,
		freelist: newFreeList(),
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
		data: make([]byte, d.pageSize),
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
