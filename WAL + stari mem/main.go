package main

import (
	Mem "NASP/Memtable"
	Wal "NASP/WAL"
	"fmt"
	"log"
	"time"
)

func put(wal *Wal.WAL, mem *Mem.Memtable, key string, value []byte) bool {
	segment, _ := wal.GetLastSegment()
	if segment.Size() > 300 {
		fmt.Println("Novi segment napravljen")
		_, err := wal.NewSegment()
		if err != nil {
			return false
		}
	}
	wal.Set([]*Wal.T{&Wal.T{key, value, false}})
	mem.Add(key, value)
	return true
}

func delete(wal *Wal.WAL, mem *Mem.Memtable, key string) { //treba provera da li postoji
	var data []byte
	segment, _ := wal.GetLastSegment()
	if segment.Size()+int64(len(data)) > 300 {
		_, _ = wal.NewSegment()
	}
	wal.Set([]*Wal.T{&Wal.T{key, []byte{}, true}})
	mem.Delete(key)
}

func main() {
	wal, err := Wal.NewWAL("C:/Users/suput/GolandProjects/NASP/data", time.Second, 2, 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = wal.Open()
	if err != nil {
		fmt.Println(err)
		return
	}
	memtable := Mem.New(10, 10000)
	fmt.Println("Program pokrenut. Segmenti su: ")
	for _, s := range wal.Segments() {
		fmt.Println(s)
	}

	/*err = wal.Set([]*Wal.T{&Wal.T{"selo", []byte{1}, false}, &Wal.T{"drzava", []byte{2}, false}})
	if err != nil {
		fmt.Println(err)
	}
	data := []byte{}
	data = append(data, wal.Process("srbija", []byte {1,1}, false)...)
	segment, _ := wal.GetLastSegment()*/

	//delete(wal, memtable, "srbija")
	/*i := 1
	fmt.Println("Sadrzaj:")
	for i <= len(wal.Segments()) {
		fmt.Println("Segment", i)
		s, err := wal.ReadConverted(int64(i))
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range s {
			fmt.Println(v)
		}
		i++
	}*/
	var key = ""
	var value = []byte{}
	// Taking input from user
	for {
		fmt.Println("Enter key: ")
		fmt.Scanln(&key)
		fmt.Println("Enter value: ")
		_, err := fmt.Scanln(&value)
		if err != nil {
			return
		}
		fmt.Println(string(value))


		put(wal, memtable, key, value)

		//delete(wal, memtable, "2")
		//i := 1
		segment, _ := wal.GetLastSegment()
		s, err := wal.ReadConverted(segment.Index())

		if err != nil {
			log.Fatal(err)
		}
		for _ , v := range s {
			fmt.Println(v)
		}
	}
	fmt.Println("Sadrzaj poslednjeg segmenta:")
	segment, _ := wal.GetLastSegment()
	s, err := wal.ReadConverted(int64(segment.Index()))
	for _, v := range s {
		fmt.Println(v)
	}
	fmt.Println("Program pokrenut. Segmenti su: ")
	for _, s := range wal.Segments() {
		fmt.Println(s)
	}
	//65536
}
