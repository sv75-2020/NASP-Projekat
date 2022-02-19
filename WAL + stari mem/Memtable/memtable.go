package Memtable

import (
"errors"
)

const (
	TOMBSTONE_SIZE = 1
	TIMESTAMP_SIZE = 16
)

type Memtable struct {
	data *SkipList
	size int
	// ovdje MOZDA treba da ima i pokazivac na WAL
}

func New(maxHeight int, size int) *Memtable {
	return &Memtable{
		data: createSkipList(maxHeight),
		size: size,
	}
}

func (m *Memtable) isFull(size int) bool {
	if size >= 10000 {
		return true
	} else {
		return false
	}
}
func (m *Memtable) Add(key string, value []byte) {
	memSize := len(key) + len(value) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	if !m.isFull(m.size + memSize) { // ako imamo i dalje dovoljno prostora na memtable-u da dodamo jos jedan zapis
		m.size = m.size + memSize // povezamo velicinu
		m.data.Add(key, value)    // dodajemo u skiplistu memtable-a
	} else {
		m.flush()
	}
}

func (m *Memtable) Delete(key string) {
	e := m.data.search(key) // dobavljamo po kljucu
	if e == nil {           // provjeravamo da li ga ima
		errors.New("Kljuc sa unesenom vrjednoscu nije postojeci! ")
	}
	//memSize := len(e.value) + len(key) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	m.data.delete(key) // tombstonujemo po kljucu
	// m.size = m.size - memSize    // smanjujemo velicinu za taj koji smo obrisali ZA OVO JOS NISMO SIGURNI NARODE

}

func (m *Memtable) Get(key string) *SkipListNode {
	return m.data.search(key)
}

func (m *Memtable) flush() {
	// provjerava se da li je dostignut kapacitet od Memtable-a
	//ako jeste ovdje treba da se pozove f-ja koja pravi sstable
}

// nesto za read path --> to radimo naknadno --> tj za trazenje

