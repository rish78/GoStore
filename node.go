package main

import (
	"bytes"
	"encoding/binary"
)

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

func (n *Node) writeNode(node *Node) (*Node, error) {
	return n.Dal.writeNode(node)
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.Dal.getNode(pageNum)
}

func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, item := range n.items {
		res := bytes.Compare(item.key, key)
		if res == 0 {
			return true, i
		}

		if res == 1 {
			return false, i
		}
	}
	return false, len(n.items)
}

func (n *Node) findKey(key []byte) (int, *Node, error) {
	index, node, err := findKeyHelper(n, key)
	if err != nil {
		return -1, nil, err
	}
	return index, node, nil
}

func findKeyHelper(n *Node, key []byte) (int, *Node, error) {
	wasFound, index := n.findKeyInNode(key)

	if wasFound {
		return index, n, nil
	}

	if n.isLeaf() {
		return -1, nil, nil
	}

	nextChild, err := n.getNode(n.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key)
}

func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumSize
	return size
}

func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.items {
		size += n.elementSize(i)
	}

	size += pageNumSize
	return size
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex {
		n.items = append(n.items, *item)
		return insertionIndex
	}

	n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
	n.items[insertionIndex] = *item
	return insertionIndex
}

func (n *Node) isOverPopulated() bool {
	return n.Dal.isOverPopulated(n)
}

func (n *Node) isUnderPopulated() bool {
	return n.Dal.isUnderPopulated(n)
}

func (n *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	splitIndex := nodeToSplit.Dal.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf() {
		newNode, _ = n.writeNode(n.Dal.newNode(convertToPointerSlice(nodeToSplit.items[splitIndex+1:]), []pgnum{}))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode, _ = n.writeNode(n.Dal.newNode(convertToPointerSlice(nodeToSplit.items[splitIndex+1:]), nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}

	n.addItem(&middleItem, nodeToSplitIndex)
	if len(n.childNodes) == nodeToSplitIndex+1 {
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNum
	}
	n.writeNodes(n, nodeToSplit)
}

func convertToPointerSlice(items []Item) []*Item {
	pointerSlice := make([]*Item, len(items))
	for i, item := range items {
		pointerSlice[i] = &item
	}
	return pointerSlice
}
