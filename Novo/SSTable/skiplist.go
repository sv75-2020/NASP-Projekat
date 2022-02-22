package SSTable

import (
	"math/rand"
	"time"
)

var nodes []*SkipListNode

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
	sorted    []*SkipListNode
}

type SkipListNode struct {
	key       string
	value     []byte
	next      []*SkipListNode
	Timestamp int64
	Tombstone bool
}

func (s *SkipListNode) Key() string {
	return s.key
}

func (s *SkipListNode) Value() []byte {
	return s.value
}

func createNode(high int, key string, value []byte, i int) *SkipListNode {
	node := &SkipListNode{
		key:       key,
		value:     value,
		next:      make([]*SkipListNode, high),
		Timestamp: time.Now().Unix(),
		Tombstone: false,
	}
	if i != 1 {
		nodes = append(nodes, node)
	}

	return node
}

func createNode1(high int, key string, value []byte, i int, timestamp int64) *SkipListNode {
	node := &SkipListNode{
		key:       key,
		value:     value,
		next:      make([]*SkipListNode, high),
		Timestamp: timestamp,
		Tombstone: false,
	}
	if i != 1 {
		nodes = append(nodes, node)
	}

	return node
}

func createSkipList(maxHeight int) *SkipList {
	head := createNode(maxHeight, "", nil, 1)
	return &SkipList{
		maxHeight: maxHeight,
		size:      0,
		height:    0,
		head:      head,
	}
}

func createSkipList1(maxHeight int) *SkipList {
	head := createNode1(maxHeight, "", nil, 1, 0)
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

func (s *SkipList) Add1(key string, value []byte, timestamp int64) {
	if s.search(key) != nil {
		n := s.search(key)
		n.value = value
		return
	}

	update := make([]*SkipListNode, s.maxHeight)
	current := s.head
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

	node := createNode1(level, key, value, 0, timestamp)
	for i := 0; i < level; i++ {
		if update[i] != nil {
			node.next[i] = update[i].next[i]
			update[i].next[i] = node
		} else {

		}
	}
	s.size++
}

func (s *SkipList) Add(key string, value []byte) {
	if s.search(key) != nil {
		n := s.search(key)
		n.value = value
		return
	}

	update := make([]*SkipListNode, s.maxHeight)
	current := s.head
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

	node := createNode(level, key, value, 0)
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
		node.Tombstone = true
		s.size--
		return true
	} else {
		return false
	}

}

func (s *SkipList) sort() {
	curr := s.head
	//s.sorted = append(s.sorted, curr)
	for ; curr.next[0] != nil; curr = curr.next[0] {
		s.sorted = append(s.sorted, curr.next[0])
	}
}

func (s *SkipList) Size() int {
	return s.size
}

/*func (s *SkipList) isFull() bool {
	if s.size == 10 { // OVA VRJEDNOST TREBA DA SE CITA IZ KONFIGURACIONOG FAJL
		return true
	} else {
		return false
	}

}*/

/*func main() {
	sl := createSkipList(10)

	sl.Add("selo", []byte("klek"))
	sl.Add("planina", []byte("jahorina"))
	sl.Add("reka", []byte("dunav"))
	sl.Add("drzava", []byte("srbija"))
	sl.Add("grad", []byte("trebinje"))

	fmt.Println("SIZE", sl.Size())
	fmt.Println(sl.search("selo"))
	fmt.Println(sl.delete("selo"))
	fmt.Println("SIZE", sl.Size())
	sl.sort()
	for _, r := range sl.sorted {
		fmt.Println(r.key)
	}

}*/
