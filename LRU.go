package main

import (
	"container/list"
	"fmt"
)

type Cache interface {
	Get(key string) string
	Set(key string, value []byte)
	Remove(key string)
}

type mapElement struct {
	element *list.Element
	value   []byte
}

type LRU struct {
	cache   map[string]*mapElement
	maxsize int
	list    list.List
}

func (l *LRU) Get(key string) []byte {
	v, ok := l.cache[key]
	if !ok {
		return nil
	}
	l.list.MoveToFront(v.element)
	return v.value
}

func (l *LRU) Remove(key string) bool {
	v, ok := l.cache[key]
	fmt.Println(v.element)
	if !ok {
		return false

	} else {
		delete(l.cache, key)
		for v.element.Next() != nil {
			v.element.Value = v.element.Next().Value
			v.element = v.element.Next()

		}
		l.list.Remove(v.element)
		return true
	}

}

func (l *LRU) Set(key string, value []byte) {
	v, ok := l.cache[key]
	if !ok {
		el := l.list.PushFront(key)
		l.cache[key] = &mapElement{
			element: el,
			value:   value,
		}

		if l.list.Len() > l.maxsize {
			backElement := l.list.Back()
			backElementKey := backElement.Value.(string)
			l.list.Remove(backElement)
			delete(l.cache, backElementKey)
		}
	} else {
		v.value = value
		l.list.MoveToFront(v.element)
	}

}

func NewLRU(size int) *LRU {
	return &LRU{
		cache:   map[string]*mapElement{},
		maxsize: size,
		list:    list.List{},
	}

}

func (l *LRU) Print() {
	for e := l.list.Front(); e != nil; e = e.Next() {
		fmt.Print(e.Value, " ")
	}
}

func main() {
	lru := NewLRU(5)
	lru.Set("a", []byte("ponedeljak"))
	lru.Set("b", []byte("utorak"))
	lru.Set("c", []byte("sreda"))
	lru.Set("d", []byte("cetvrtak"))
	lru.Get("a")
	lru.Get("c")
	lru.Set("l", []byte("ponedeljak"))
	lru.Set("m", []byte("utorak"))
	lru.Remove("m")
	lru.Set("v", []byte("endzi"))

	lru.Print()

}
