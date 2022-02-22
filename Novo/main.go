package main

import (
	cache "Novo/Cache"
	config "Novo/Config"
	Mem "Novo/SSTable"
	tb "Novo/TokenBucket"
	Wal "Novo/WAL"
	"bufio"
	"fmt"
	"os"
	"time"
)

func createWAL() *Wal.WAL {
	wal, err := Wal.NewWAL("data", time.Second, 2)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	err = wal.Open()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return wal
}

func main() {
	t := tb.NewTokenBucket()
	c := config.Config{}
	c.ReadConfigFile()
	scanner := bufio.NewScanner(os.Stdin)
	wal := createWAL()
	memtable := Mem.NewM(15)
	Mem.Load()
	lru := cache.NewLRU()
	for {
		fmt.Println("1)Unos\n2)Traženje\n3)Brisanje")
		fmt.Print("Izaberite radnju: ")
		scanner.Scan()
		if scanner.Text() == "1" { // write path
			if t.Reset() == true {
			fmt.Println("UNOS PODATKA")
			fmt.Println("--------------")
			fmt.Println("Unesite ključ: ")
			scanner.Scan()
			k := scanner.Text()
			fmt.Println("Unesite vrednost:")
			scanner.Scan()
			v := scanner.Text()
			Wal.Put(wal, memtable, k, []byte(v))
			lru.Set(k, []byte(v))
			} else {
			fmt.Println("Sačekajte 60s, prethodni zahtevi su u obradi!")
			}
		} else if scanner.Text() == "2" { // read path
			if t.Reset() == true {
				fmt.Println("TRAŽENJE PODATKA")
				fmt.Println("--------------")
				fmt.Println("Unesite ključ: ")
				scanner.Scan()
				k := scanner.Text()
				v, err := Get(memtable, lru, k)
				if !err {									// boolean
					fmt.Println("Vrednost sa ovim ključem ne postoji.")
				} else {
					fmt.Println("Vrednost traženog elementa sa ključem", k, "je:",  string(v))
					lru.Set(k, v)
				}
			} else {
				fmt.Println("Sačekajte 60s, prethodni zahtevi su u obradi!")
			}
		} else if scanner.Text() == "3" {
			if t.Reset() == true {
				fmt.Println("BRISANJE PODATKA.")
				fmt.Println("--------------")
				fmt.Println("Unesite ključ: ")
				scanner.Scan()
				k := scanner.Text()
				found := memtable.Get(k)
				if found != nil {
					Wal.Delete(wal, memtable, k)
				} else {
					fmt.Println("Element se ne može obrisati jer ne postoji.")
				}
			} else {
				fmt.Println("Sačekajte 60s, prethodni zahtevi su u obradi!")
			}
		} else {
			fmt.Println("Unesite validnu radnju.")
		}
		if scanner.Err() != nil {
			fmt.Println("Error: ", scanner.Err())
		}
		Mem.WriteFileNames()
	}

}

func Get(memtable *Mem.Memtable, lru *cache.LRU, key string) ([]byte, bool) {
	e := memtable.Get(key)
	if e != nil {
		if !e.Tombstone {
			return e.Value(), true
		} else {
			fmt.Println("Podatak je obrisan.")
			return []byte{}, false
		}
	}
	e1 := lru.Get(key)
	if e1 != nil {
		fmt.Println("Pronađeno u cache.")
		return e1, true
	}
	e2, file := Mem.FindBloom(key) // trazi u disku
	if e2 {
		fmt.Println("Pronađeno na disku.")
		sumFileName := file[0:14] + "summaryFile" + file[22:]
		indFileName := file[0:14] + "indexFile" + file[22:]
		v:= Mem.Get(key, sumFileName, indFileName, file)
		return []byte(v), true
	}
	return nil, false

}
