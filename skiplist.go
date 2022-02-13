package main

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	key       string
	value     []byte
	next      []*SkipListNode
	timestamp int64
	tombstone bool
}

func (s *SkipListNode) Key() string {
	return s.key
}

func (s *SkipListNode) Value() []byte {
	return s.value
}

func createNode(high int, key string, value []byte) *SkipListNode {
	return &SkipListNode{
		key:       key,
		value:     value,
		next:      make([]*SkipListNode, high),
		timestamp: time.Now().Unix(),
		tombstone: false,
	}
}

func createSkipList(maxHeight int) *SkipList {
	head := createNode(maxHeight, "", nil)
	return &SkipList{
		maxHeight: maxHeight,
		size:      0,
		height:    0,
		head:      head,
	}
}

//pomocna funkcija za bacanje novcica
func (s *SkipList) roll() int {
	return rand.Intn(10)
}

func (s *SkipList) Add(key string, value []byte) {
	if s.search(key) != nil {
		fmt.Println("Kljuc postoji")
		return
	}
	update := make([]*SkipListNode, s.maxHeight)
	current := s.head // trenutni glava
	for i := s.height - 1; i >= 0; i-- {
		for current != nil && current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current

	}

	level := s.roll()

	//azuriramo head.next pokazivac
	if level > s.height {
		for i := s.height; i < level; i++ {
			update[i] = s.head
		}
		s.height = level
	}

	node := createNode(level, key, value)
	for i := 0; i < level; i++ {
		if update[i] != nil {
			node.next[i] = update[i].next[i]
			update[i].next[i] = node
		} else {

		}
	}
	s.size++
}

func (s *SkipList) search(key string) *SkipListNode {
	curr := s.head
	for i := s.height - 1; i >= 0; i-- {
		for curr != nil && curr.next[i] != nil && curr.next[i].key <= key {
			if curr.next[i].key == key {
				return curr.next[i]
			}
			curr = curr.next[i]

		}
	}
	return nil
}

func (s *SkipList) delete(key string) bool {
	node := s.search(key)
	if node != nil {
		node.tombstone = false
		s.size--
		return true
	} else {
		return false
	}

}

func (s *SkipList) Size() int {
	return s.size
}

func main() {
	sl := createSkipList(10)
	sl.Add("drzava", []byte("srbija"))
	sl.Add("grad", []byte("trebinje"))
	sl.Add("selo", []byte("klek"))
	sl.Add("reka", []byte("dunav"))
	sl.Add("planina", []byte("jahorina"))

	fmt.Println("SIZE", sl.Size())
	fmt.Println(sl.search("selo"))
	fmt.Println(sl.delete("selo"))
	fmt.Println("SIZE", sl.Size())

}
