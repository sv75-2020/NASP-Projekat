package SSTable

import (
	config "Novo/Config"
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	TimestampSize = 16
)

type Memtable struct {
	data *SkipList
	size int
	// ovdje MOZDA treba da ima i pokazivac na WAL
}

func NewM(maxHeight int) *Memtable {
	return &Memtable{
		data: createSkipList(maxHeight),
		size: 0,
	}
}

func (m *Memtable) isFull(size int) bool {
	c := config.Config{}
	c.ReadConfigFile()
	if size >= c.GetSizeMem() {
		return true
	} else {
		return false
	}
}
func (m *Memtable) Add(key string, value []byte) {
	memSize := len(key) + len(value) + TOMBSTONE_SIZE + TimestampSize
	if !m.isFull(m.size + memSize) { // ako imamo i dalje dovoljno prostora na memtable-u da dodamo jos jedan zapis
		m.size = m.size + memSize // povezamo velicinu
		m.data.Add(key, value)    // dodajemo u skiplistu memtable-a
	} else {
		m.flush()
		m.size = m.size + memSize // povezamo velicinu
		m.data.Add(key, value)    // dodajemo u skiplistu memtable-a
	}
}

func (m *Memtable) Delete(key string) {
	e := m.data.search(key) // dobavljamo po kljucu
	if e == nil {           // provjeravamo da li ga ima
		fmt.Println("Ključ ne postoji.")
	} else {
		m.data.delete(e.key)
		fmt.Println("Uspešno ste obrisali element.")
	}

}

func (m *Memtable) Get(key string) *SkipListNode {
	if m.data.search(key) != nil {
		return m.data.search(key)
	}
	return nil

}

var dataNames []string
var indexNames []string
var summaryNames []string

//koja cuva sve bloom filtere
var bloomNames []string
var merkleNames []string

func Load() {
	brojaciNivo = []int{0, 0, 0}
	file, _ := os.OpenFile("SSTable/files/ssTables.txt", os.O_RDWR, 0777)
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	if scanner.Text() != "" {
		d := strings.Split(scanner.Text(), "|")
		for i := range brojaciNivo {
			brojaciNivo[i], _ = strconv.Atoi(d[i])
		}
	}
	for scanner.Scan() {
		if scanner.Text() != "" {
			s := strings.Split(scanner.Text(), "|")
			dataNames = append(dataNames, s[0])
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func WriteFileNames() {
	file, _ := os.OpenFile("SSTable/files/ssTables.txt", os.O_RDWR, 0777)
	_, _ = file.WriteString(strconv.Itoa(brojaciNivo[0]) + "|" + strconv.Itoa(brojaciNivo[1]) + "|" + strconv.Itoa(brojaciNivo[2]) + "\n")
	for i := 0; i < len(dataNames); i++ {
		_, err := file.WriteString(dataNames[i] + "\n")
		if err != nil {
			return
		}
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

}

var allnodes []Node

func (m *Memtable) flush() {
	fmt.Println("Zapisano na disk.")
	var DataFileName = "SSTable/files/dataFile_1"
	var IndexFileName = "SSTable/files/indexFile_1"
	var SummaryFileName = "SSTable/files/summaryFile_1"
	//napravili bloom
	var BloomFileName = "SSTable/files/bloomFile_1"
	//napravi merkle
	var MerkleFileName = "SSTable/files/merkleFile_1"
	m.data.sort()
	/*summary := NewSummary(m.data.sorted)
	index := NewIndex()*/
	bf := New(uint(len(m.data.sorted)))
	var i = brojaciNivo[0] + 1
	DataFileName += "_" + strconv.Itoa(i) + ".txt"
	IndexFileName += "_" + strconv.Itoa(i) + ".txt"
	SummaryFileName += "_" + strconv.Itoa(i) + ".txt"
	//napravili bloom
	BloomFileName += "_" + strconv.Itoa(i) + ".txt"
	//napravili merkle
	MerkleFileName += "_" + strconv.Itoa(i) + ".txt"

	dataNames = append(dataNames, DataFileName)
	indexNames = append(indexNames, IndexFileName)
	summaryNames = append(summaryNames, SummaryFileName)
	bloomNames = append(bloomNames, BloomFileName)
	merkleNames = append(merkleNames, MerkleFileName)

	dFile, _ := os.Create(DataFileName)
	err := dFile.Close()
	if err != nil {
		return
	}

	iFile, _ := os.Create(IndexFileName)
	err = iFile.Close()
	if err != nil {
		return
	}

	sFile, _ := os.Create(SummaryFileName)
	err = sFile.Close()
	if err != nil {
		return
	}

	//kreirali bloom
	bFile, _ := os.Create(BloomFileName)
	err = bFile.Close()
	if err != nil {
		return
	}

	//kreirali merkle
	mFile, _ := os.Create(MerkleFileName)
	err = mFile.Close()
	if err != nil {
		return
	}

	//dodali parametar bloom u make
	MakeData(m.data.sorted, bf, DataFileName, IndexFileName, SummaryFileName, BloomFileName)
	for _, n := range m.data.sorted {
		n := Node{Left: nil, Right: nil, Data: Hash(n.value)}
		allnodes = append(allnodes, n)
	}
	root := MerkleTree(allnodes)
	if root == nil {
		fmt.Println("Nije moguce upisati.")
	} else {
		WriteToFile(root.Root, MerkleFileName)
	}
	allnodes = nil
	brojaciNivo[0]++
	if brojaciNivo[0] == 2 {
		compaction()
	}
	m.size = 0
	m.data = createSkipList(15)
}

var brojaciNivo []int

func compaction() {
	for i := 1; i <= 3; i++ {
		//k:=0
		if brojaciNivo[i-1] == 2 && i != 3 {
			merge(i)
			break
		}
	}

}

func remove(slice []string, name string) {
	slice1 := []string{}
	slice2 := []string{}
	for i, n := range slice {
		if n == name {
			slice1 = append(slice[:i], slice[i+1:]...)
			break
		}
	}

	dataNames = append(slice2, slice1...)
}

func merge(nivo int) {
	br := brojaciNivo[nivo] + 1
	newFile, _ := os.Create("SSTable/files/dataFile_" + strconv.Itoa(nivo+1) + "_" + strconv.Itoa(br) + ".txt")
	indexFile, _ := os.Create("SSTable/files/indexFile_" + strconv.Itoa(nivo+1) + "_" + strconv.Itoa(br) + ".txt")
	summaryFile, _ := os.Create("SSTable/files/summaryFile_" + strconv.Itoa(nivo+1) + "_" + strconv.Itoa(br) + ".txt")
	//dodali bloom
	bloomFile, _ := os.Create("SSTable/files/bloomFile_" + strconv.Itoa(nivo+1) + "_" + strconv.Itoa(br) + ".txt")
	//dodali merkle
	merkleFile, _ := os.Create("SSTable/files/merkleFile_" + strconv.Itoa(nivo+1) + "_" + strconv.Itoa(br) + ".txt")

	dataNames = append(dataNames, newFile.Name())
	spojiFajlove(nivo, newFile, indexFile, summaryFile, bloomFile, merkleFile)
	brojaciNivo[nivo-1] = 0
	brojaciNivo[nivo]++
	if brojaciNivo[nivo] == 2 && nivo != 2 { // proverava broj fajlova na sledećem nivou, i ne treba da pozove merge ako je na 3. nivou tj ako je nivo 2
		merge(nivo + 1)
	}
}

func spojiFajlove(nivo int, newFile *os.File, indexFile *os.File, summaryFile *os.File, bloomFile *os.File, merkleFile *os.File) {
	file1, _ := os.OpenFile("SSTable/files/dataFile_"+strconv.Itoa(nivo)+"_1.txt", os.O_RDWR, 0777)
	file2, _ := os.OpenFile("SSTable/files/dataFile_"+strconv.Itoa(nivo)+"_2.txt", os.O_RDWR, 0777)

	size, _ := FileLen(file1)
	data := make([]byte, size)
	_, err := file1.Read(data)
	if err != nil {
		panic(err)
	}

	size1, _ := FileLen(file2)
	data1 := make([]byte, size1)
	_, err = file2.Read(data1)
	if err != nil {
		panic(err)
	}

	lista := createSkipList1(15)
	i := uint64(0)
	for i < uint64(len(data)) {
		timestamp := binary.LittleEndian.Uint64(data[i+C_SIZE : i+CRC_SIZE])
		tombstone := data[i+CRC_SIZE]
		key_size := binary.LittleEndian.Uint64(data[i+TOMBSTONE_SIZE : i+KEY_SIZE])
		value_size := binary.LittleEndian.Uint64(data[i+KEY_SIZE : i+VALUE_SIZE])
		key_data := string(data[i+VALUE_SIZE : i+VALUE_SIZE+key_size])
		value_data := data[i+VALUE_SIZE+key_size : i+VALUE_SIZE+key_size+value_size]

		i = i + VALUE_SIZE + key_size + value_size
		podaci2 := drugaTabela(data1, key_data, lista)
		if podaci2 == nil && tombstone != 1 {
			lista.Add1(key_data, value_data, int64(timestamp))
		} else if podaci2 != nil {
			if string(podaci2) != "obrisan" {
				lista.Add1(string(podaci2[VALUE_SIZE:VALUE_SIZE+key_size]), podaci2[VALUE_SIZE+key_size:VALUE_SIZE+key_size+value_size], int64(binary.LittleEndian.Uint64(data[C_SIZE:CRC_SIZE])))
			}
		}
	}
	lista.sort()
	/*newIndex := NewIndex()
	newSummary := NewSummary(lista.sorted)*/
	newBloom := New(uint(lista.size))
	MakeData(lista.sorted, newBloom, newFile.Name(), indexFile.Name(), summaryFile.Name(), bloomFile.Name())
	//dodali merkle
	var togethernodes []Node
	for _, n:= range lista.sorted {
		n := Node{Left: nil, Right: nil, Data: Hash(n.value)}
		togethernodes = append(togethernodes, n)
	}
	root := MerkleTree(togethernodes)
	if root != nil {
		WriteToFile(root.Root, merkleFile.Name())
	}
	togethernodes = nil
	err = merkleFile.Close()
	if err != nil {
		return
	}
	err = newFile.Close()
	if err != nil {
		return
	}
	err = file1.Close()
	if err != nil {
		return
	}
	err = indexFile.Close()
	if err != nil {
		return
	}
	err = summaryFile.Close()
	if err != nil {
		return
	}
	err = bloomFile.Close()
	if err != nil {
		return
	}

	err = os.Remove("SSTable/files/dataFile_" + strconv.Itoa(nivo) + "_1.txt")
	err = os.Remove("SSTable/files/indexFile_" + strconv.Itoa(nivo) + "_1.txt")
	err = os.Remove("SSTable/files/summaryFile_" + strconv.Itoa(nivo) + "_1.txt")
	err = os.Remove("SSTable/files/bloomFile_" + strconv.Itoa(nivo) + "_1.txt")
	err = os.Remove("SSTable/files/merkleFile_" + strconv.Itoa(nivo) + "_1.txt")
	if err != nil {
		log.Fatal(err)
	}
	err = file2.Close()
	if err != nil {
		return
	}
	err = os.Remove("SSTable/files/dataFile_" + strconv.Itoa(nivo) + "_2.txt")
	err = os.Remove("SSTable/files/indexFile_" + strconv.Itoa(nivo) + "_2.txt")
	err = os.Remove("SSTable/files/summaryFile_" + strconv.Itoa(nivo) + "_2.txt")
	err = os.Remove("SSTable/files/bloomFile_" + strconv.Itoa(nivo) + "_2.txt")
	err = os.Remove("SSTable/files/merkleFile_" + strconv.Itoa(nivo) + "_2.txt")
	if err != nil {
		log.Fatal(err)
	}

	remove(dataNames, "SSTable/files/dataFile_"+strconv.Itoa(nivo)+"_1.txt")
	remove(dataNames, "SSTable/files/dataFile_"+strconv.Itoa(nivo)+"_2.txt")

}

func drugaTabela(data []byte, key string, lista *SkipList) []byte {
	i := uint64(0)
	for i < uint64(len(data)) {
		timestamp := binary.LittleEndian.Uint64(data[i+C_SIZE : i+CRC_SIZE])
		tombstone := data[i+CRC_SIZE]
		key_size := binary.LittleEndian.Uint64(data[i+TOMBSTONE_SIZE : i+KEY_SIZE])
		value_size := binary.LittleEndian.Uint64(data[i+KEY_SIZE : i+VALUE_SIZE])
		key_data := string(data[i+VALUE_SIZE : i+VALUE_SIZE+key_size])
		value_data := data[i+VALUE_SIZE+key_size : i+VALUE_SIZE+key_size+value_size]
		if key == key_data {
			if tombstone == 1 {
				return []byte("obrisan")
			}
			return data[i : i+VALUE_SIZE+key_size+value_size]
		}
		if tombstone != 1 {
			lista.Add1(key_data, value_data, int64(timestamp))
		}
		i = i + VALUE_SIZE + key_size + value_size
	}
	return nil
}

func FindBloom(key string) (bool, string) {
	for i := 1; i <= len(dataNames); i++ {
		lastFile := dataNames[len(dataNames)-i]
		blFileName := lastFile[0:14] + "bloomFile" + lastFile[22:]
		bf := loadBloom(blFileName)
		if bf == nil {
			return false, ""
		}
		a := bf.Search(key)
		if a {
			return a, lastFile
		}

	}
	return false, ""
}

/*func main() {
	load()
	fmt.Println(dataNames)
	memtable := NewM(15)
	memtable.Add("drzava", []byte("srbija"))
	memtable.Add("selo", []byte("klek"))
	memtable.Add("reka", []byte("dunav"))
	memtable.Add("planina", []byte("jahorina"))
	memtable.Add("cvet", []byte("jahorin1"))
	memtable.Delete("selo")
	var key=""
	for i:=0;i<4;i++{
		fmt.Println("Enter key: ")
		fmt.Scanln(&key)
		memtable.Add(key, []byte(key))
	}
	memtable.Delete("3")
	writeFileNames()

}*/

//EVOOO GAAA
//place mi se - M
//nzm br sto nmg da pricam msm ne cujete me
