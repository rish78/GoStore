package main

import "bytes"

type Collection struct {
	name []byte
	root pgnum

	*Dal
}

func newCollection(name []byte, root pgnum) *Collection {
	return &Collection{
		name: name,
		root: root,
	}
}

func (c *Collection) Find(key []byte) (*Item, error) {
	n, err := c.Dal.getNode(c.root)
	if err != nil {
		return nil, err
	}

	index, node, err := n.findKey(key)
	if err != nil {
		return nil, err
	}

	return &node.items[index], nil
}

func (c *Collection) Insert(key []byte, value []byte) error {
	i := newItem(key, value)

	var root *Node
	var err error
	if c.root == 0 {
		root, err = c.Dal.writeNode(c.Dal.newNode([]*Item{i}, []pgnum{}))
		if err != nil {
			return err
		}
		c.root = root.pageNum
		return nil
	} else {
		root, err = c.Dal.getNode(c.root)
		if err != nil {
			return err
		}
	}

	insertionIndex, insertionNode, err := root.findKey(key)
	if err != nil {
		return err
	}

	if insertionNode.items != nil && bytes.Compare(insertionNode.items[insertionIndex].key, key) == 0 {
		insertionNode.items[insertionIndex] = *i
	} else {
		insertionNode.addItem(i, insertionIndex)
	}

	_, err = c.Dal.writeNode(insertionNode)

	if err != nil {
		return err
	}

	//rebalancing

	return nil
}
