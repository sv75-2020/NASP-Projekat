package WAL

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/MilosSimic/lru"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (wal *WAL) Process(key string, value []byte, deleted bool) []byte { //pretvara iz vrednosti u bajtove
	data := []byte{}

	crcb := make([]byte, C_SIZE)
	binary.LittleEndian.PutUint32(crcb, CRC32(string(value)))
	data = append(data, crcb...)

	sec := time.Now().Unix()
	secb := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(secb, uint64(sec))
	data = append(data, secb...)

	//0 alive 1 deleted
	if deleted {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}

	keyb := []byte(key)
	keybs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	valuebs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(valuebs, uint64(len(value)))

	data = append(data, keybs...)
	data = append(data, valuebs...)

	data = append(data, key...) //!!!!!!!!!!!!!!!!
	data = append(data, value...)

	return data
}

func (wal *WAL) convert(data []byte) []Entry { //konvertuje niz bajtova u niz entrya
	rez := []Entry{}
	if len(data) == 0 {
		return rez
	}

	i := uint64(0)
	for i < uint64(len(data)) {
		crc := binary.LittleEndian.Uint32(data[i : i+C_SIZE])
		timestamp := binary.LittleEndian.Uint64(data[i+C_SIZE : i+CRC_SIZE])
		tombstone := data[i+CRC_SIZE]
		key_size := binary.LittleEndian.Uint64(data[i+TOMBSTONE_SIZE : i+KEY_SIZE])
		value_size := binary.LittleEndian.Uint64(data[i+KEY_SIZE : i+VALUE_SIZE])
		key_data := string(data[i+VALUE_SIZE : i+VALUE_SIZE+key_size])
		val := data[i+VALUE_SIZE+key_size : i+VALUE_SIZE+key_size+value_size]

		b := false //brisanje
		if tombstone == 1 {
			b = true
		}

		e := Entry{
			crc,
			timestamp,
			b,
			key_data,
			val,
		}
		rez = append(rez, e)

		// valculate new index
		i = i + VALUE_SIZE + key_size + value_size //pomeri za duzinu celog reda
	}
	return rez
}

func (wal *WAL) Update(data []byte, s *Segment) error { //dodaje novi podatak u segment
	f, err := os.OpenFile(s.Path(), os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()
	err = append_entry(f, data)
	if err != nil {
		return err
	}
	s.Append(data, int64(len(data)))
	return nil
}

func (wal *WAL) Set(ts []*T) error { // dodaje novi segment u koj upisuje novi niz podataka
	tail := wal.segments[wal.lastIndex-1]
	data := []byte{}
	if len(ts) == 1 {
		t := ts[0]
		data = append(data, wal.Process(t.Key, t.Value, t.Deleted)...)
	} else {
		for _, t := range ts {
			part := wal.Process(t.Key, t.Value, t.Deleted)
			data = append(data, part...)
		}
	}
	err := wal.Update(data, tail)

	if err != nil {
		return err
	}
	return nil
}

func (wal *WAL) Open() error { //pamti segmente na disku i poslednji smesta u memoriju
	err := filepath.Walk(wal.path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".wal" {
			return nil
		}

		name := fileNameWithoutExtension(info.Name())
		var i int64
		if strings.HasSuffix(name, END_EXT) {
			lastIndex := fileNameWithoutSuffix(name, END_EXT)
			i, err = convertIndex(lastIndex)
			if err != nil {
				return err
			}
			wal.lastIndex = i
		} else {
			i, err = convertIndex(name)
			if err != nil {
				return err
			}
		}

		fi, err := os.Stat(path)
		if err != nil {
			return err
		}

		segment := &Segment{
			path:  path,
			index: i,
			size:  fi.Size(),
		}
		wal.AppendSegment(segment)
		return nil
	})
	if err != nil {
		return err
	}
	return wal.setupLastSegment()

}

func (wal *WAL) setupLastSegment() error { //otvara poslednji segment u memoriju-tail
	lastSegment, err := wal.GetLastSegment()
	if err == nil {
		//Open file
		f, _:= os.OpenFile(lastSegment.Path(), os.O_RDWR|os.O_CREATE, 0644)
		defer f.Close()
		result, _ := read(f)
		//Fill data to memory from last segment
		lastSegment.SetData(result)
	}
	return err
}

func (wal *WAL) GetLastSegment() (*Segment, error) { //vraca poslednji segment
	i := sort.Search(len(wal.segments), func(i int) bool { return wal.lastIndex <= wal.segments[i].index })
	if i < len(wal.segments) && wal.segments[i].index == wal.lastIndex {
		return wal.segments[i], nil
	} else {
		return wal.NewSegment()
	}
}

func (wal *WAL) NewSegment() (*Segment, error) { //pravi novi segment i otvara ga u tail
	if wal.lastIndex != 0 {
		prevTail := wal.segments[wal.lastIndex-1].Path()
		//Rename previous last segment and remove _END mark and append to new one
		regularPath := strings.Replace(prevTail, END_EXT, "", -1)
		err := os.Rename(prevTail, regularPath)
		if err != nil {
			return nil, err
		}
		s, _ := wal.GetLastSegment()
		s.path = regularPath


	}

	//Kreira novi segment i njegovu putanju
	index := int64(wal.lastIndex + 1)
	temp := fmt.Sprintf(FORMAT_NAME, index)
	temp = temp[len(temp)-20:]
	temp = strings.Join([]string{wal.path, temp}, string(os.PathSeparator))
	temp = strings.Join([]string{temp, END_EXT}, "")
	temp = strings.Join([]string{temp, WAL_EXT}, ".")


	segment := &Segment{
		index: index,
		path:  temp,
	}
	wal.lastIndex = index

	wal.AppendSegment(segment)
	err := wal.setupLastSegment()
	if err != nil {
		return nil, err
	}
	return segment, nil
}

func (wal *WAL) cleanLog() { //brise segmente pre lowmarka
	//wal.mu.Lock()
	//defer wal.mu.Unlock()

	for i := len(wal.segments) - 1; i >= wal.lowMark; i-- { //rekla tamara da poslednji ne treba da ima _end
		err := os.Remove(wal.segments[i].Path())
		if err != nil {
			fmt.Println(err)
			return
		}
		wal.removeIndex(i)
	}
}

func (wal *WAL) clean(ctx context.Context) { // kad istekne odredjeno vreme brise segmente
	go func() {
		for {
			select {
			case <-time.Tick(wal.d):
				wal.cleanLog()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func NewWAL(path string, duration time.Duration, lowMark int, cap int) (*WAL, error) { //konstruktor
	cache, err := lru.NewLRU(cap, nil)
	if err != nil {
		return nil, err
	}

	return &WAL{
		path:      path,
		segments:  []*Segment{},
		d:         duration,
		lowMark:   lowMark,
		lastIndex: 0,
		cache:     cache,
	}, nil
}

func (wal *WAL) Read(index int64) ([]byte, error) {
	// Test the last segment first
	if index >= wal.lastIndex {
		segment, err := wal.GetLastSegment()
		if err != nil {
			return nil, err
		}
		return segment.getSegmentData()
	}

	// Test cache
	cached, err := wal.findInCache(index)
	if err == nil {
		return cached, nil
	}

	//search in all segments
	segment, err := wal.findSegment(index)
	if err != nil {
		return nil, err
	}
	data, _ := segment.getSegmentData()
	return data, nil
}
func (wal *WAL) findSegment(index int64) (*Segment, error) { //trazi segment i kesira ga
	if index <= int64(len(wal.segments)) && index > 0 {
		//segment exists, cache it for the next time
		data, err := wal.segments[index].getSegmentData()
		if err == nil {
			wal.cacheit(index, data)
		}
		return wal.segments[index], nil
	} else {
		return nil, errors.New("Segment do not exists")
	}
}
func (wal *WAL) findInCache(index int64) ([]byte, error) { //trazi u kesu i vraca vrednost na prosledjenom kljucu
	key := strconv.Itoa(int(index))
	v, ok := wal.cache.Get(key)
	if !ok {
	return nil, errors.New("Cache miss!")
	}
	val := v.(*lru.Elem).Value
	s, ok := val.([]byte)
	if !ok {
	return nil, errors.New("Conversion error")
	}
	return s, nil
}

func (wal *WAL) cacheit(index int64, value []byte) error { //stavlja u kes
	key := strconv.Itoa(int(index))
	_, ok := wal.cache.Put(key, value)
	if !ok {
		return errors.New("Cache error")
	}
	return nil
}

func (tail *Segment) getSegmentData() ([]byte, error) { //dobavljanje podataka iz zadnjeg segmenta koji je u memoriji
	f, err := os.OpenFile(tail.Path(), os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()

	if err != nil {
		return nil, err
	}
	data, err := read(f)
	if err != nil {
		log.Fatal(err)
	}
	return data, nil
}

func (wal *WAL) ReadConverted(index int64) ([]Entry, error) { // konvertuje ceo segment u niz entrya
	bytes, err := wal.Read(index)
	if err != nil {
		return nil, err
	}
	return wal.convert(bytes), nil
}


