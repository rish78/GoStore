package main

import "encoding/binary"

type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	*Dal

	pageNum    pgnum
	items      []Item
	childNodes []pgnum
}

func NewEmptyNode() *Node {
	return &Node{}
}

func newItem(key, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(n.items) - 1

	isLeaf := n.isLeaf()
	var isLeafFlag uint64
	if isLeaf {
		isLeafFlag = 1
	}
	buf[leftPos] = byte(isLeafFlag)
	leftPos++

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isLeaf {
			childNode := n.childNodes[i]

			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		keyLen := len(item.key)
		valLen := len(item.value)

		offset := rightPos - keyLen - valLen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= valLen
		copy(buf[rightPos:], item.value)

		rightPos--
		buf[rightPos] = byte(valLen)

		rightPos -= keyLen
		copy(buf[rightPos:], item.key)

		rightPos--
		buf[rightPos] = byte(keyLen)

	}

	if !isLeaf {
		lastChildNode := n.childNodes[len(n.childNodes)-1]

		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}
	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftPos := 0

	isLeaf := uint16(buf[0])
	itemsCnt := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	for i := 0; i < itemsCnt; i++ {
		if isLeaf == 0 {
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, pgnum(pageNum))
		}

		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		keyLen := uint16(buf[int(offset)])
		offset++
		key := buf[offset : offset+keyLen]
		offset += keyLen

		valLen := uint16(buf[int(offset)])
		offset++
		val := buf[offset : offset+valLen]
		offset += valLen

		n.items = append(n.items, *newItem(key, val))
	}

	if isLeaf == 0 {
		pageNum := pgnum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}