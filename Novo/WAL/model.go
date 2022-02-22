package WAL

import (
	lru "Novo/Cache"
	"strconv"
	"time"
)

type WAL struct {
	path      string     //putanja do fajla sa walom
	segments  []*Segment //segmenti
	d         time.Duration
	lowMark   int      //do kog indeksa se brisu segmenti
	lastIndex int64    //indeks poslednjeg zapisa u walu
	cache     *lru.LRU //kes
}

type Segment struct { //segment
	path  string //putanja do fajla segmenta
	index int64  //pocetak segnemta
	size  int64
	data  []byte
}

type Entry struct { //red u walu
	Crc       uint32
	Timestamp uint64
	Deleted   bool
	Key       string
	Value     []byte
}

type T struct { //jedan podatak
	Key     string
	Value   []byte
	Deleted bool
}

func convertIndex(index string) (int64, error) {
	i, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return -1, err
	}
	return i, nil
}

// Funkcije za segmente

func (s *Segment) SetData(data []byte) { // Pamti datu samo poslednjem segmentu koji je u memoriji
	s.data = append(s.data, data...)
}

func (s *Segment) Path() string {
	return s.path
}

func (s *Segment) Data() []byte {
	return s.data
}

func (s *Segment) Append(data []byte, size int64) { //upis novog podatka u segment
	s.data = append(s.data, data...)
	s.size = s.size + size
}

func (s *Segment) Size() int64 {
	return s.size
}

func (s *Segment) Index() int64 {
	return s.index
}

// Funkcije za WAL

func (wal *WAL) removeIndex(index int) { //izbacuje neki segment
	wal.segments = append(wal.segments[:index], wal.segments[index+1:]...)
}

func (wal *WAL) Segments() []*Segment {
	return wal.segments
}

func (wal *WAL) AppendSegment(segment *Segment) { //dodaje novi segment na listu segmenata
	wal.segments = append(wal.segments, segment)
}
