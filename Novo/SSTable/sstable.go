package SSTable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
)

const (
	T_SIZE = 8
	C_SIZE = 4 //crc

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)

func FileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func ConvertToBinNode(n *SkipListNode) []byte { //pretvara iz vrednosti u bajtove
	data := []byte{}
	crcb := make([]byte, C_SIZE)
	binary.LittleEndian.PutUint32(crcb, CRC32(string(n.Value())))
	data = append(data, crcb...)

	secb := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(secb, uint64(n.Timestamp))
	data = append(data, secb...)

	//0 alive 1 deleted
	if n.Tombstone {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}

	keyb := []byte(n.Key())
	keybs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	valuebs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(valuebs, uint64(len(n.Value())))

	data = append(data, keybs...)
	data = append(data, valuebs...)

	data = append(data, keyb...)
	data = append(data, n.Value()...)

	return data
}

func AddIndex(position int64, key string, index *os.File) int64 {
	data := []byte{}
	keyb := []byte(key)
	positionb := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(positionb, uint64(position))
	data = append(data, keyb...)
	data = append(data, positionb...)
	lenIndex, _ := FileLen(index)
	_, err := index.Write(data)
	if err != nil {
		return 0
	}
	return lenIndex
}

func AddSummary(position int64, key string, summary *os.File) {
	data := []byte{}
	keyb := []byte(key)
	keybs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	positionb := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(positionb, uint64(position))

	data = append(data, keybs...)
	//data = append(data, valuebs...)

	data = append(data, keyb...)
	data = append(data, positionb...)
	_, err := summary.Write(data)
	if err != nil {
		return
	}
}

func MakeData(nodes []*SkipListNode, bf *BloomFilter, DataFileName string, IndexFileName string, SummaryFileName string, BloomFileName string) {
	fileIndex, err := os.OpenFile(IndexFileName, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer fileIndex.Close()

	fileSummary, err := os.OpenFile(SummaryFileName, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer fileSummary.Close()
	prvi := make([]byte, T_SIZE)
	drugi := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(prvi, uint64(len([]byte(nodes[0].Key()))))
	binary.LittleEndian.PutUint64(drugi, uint64(len([]byte(nodes[len(nodes)-1].Key()))))
	fileSummary.Write(prvi)
	fileSummary.Write(drugi)
	fileSummary.Write([]byte(nodes[0].Key()))
	fileSummary.Write([]byte(nodes[len(nodes)-1].Key()))

	//OVDE DODALI BLOOM
	fileBloom, err := os.OpenFile(BloomFileName, os.O_WRONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer func(fileBloom *os.File) {
		err := fileBloom.Close()
		if err != nil {

		}
	}(fileBloom)

	file, err := os.OpenFile(DataFileName, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for _, node := range nodes {
		bf.Add(node.Key(), fileBloom)
		position, _ := FileLen(file)
		_, err = file.Write(ConvertToBinNode(node))
		if err != nil {
			return
		}
		positionSum := AddIndex(position, node.Key(), fileIndex)
		AddSummary(positionSum, node.Key(), fileSummary)
	}
	err = file.Sync()
	if err != nil {
		return
	}
}

type SStableSummary struct {
	First string
	Last  string
}

type SSTableIndex struct {
	mapIndex map[string]int64
}

func NewIndex() *SSTableIndex {
	return &SSTableIndex{
		mapIndex: map[string]int64{},
	}
}

func NewSummary(nodes []*SkipListNode) *SStableSummary {
	first := nodes[0].Key()
	last := nodes[len(nodes)-1].Key()
	return &SStableSummary{
		First: first,
		Last:  last,
	}
}

func loadSummary(summary *os.File) *SStableSummary {
	velicinaPrvog := make([]byte, T_SIZE)
	velicinaDrugog := make([]byte, T_SIZE)

	_, _ = summary.Read(velicinaPrvog)
	velicina1 := int64(binary.LittleEndian.Uint64(velicinaPrvog))
	_, _ = summary.Read(velicinaDrugog)
	velicina2 := int64(binary.LittleEndian.Uint64(velicinaDrugog))
	prvi := make([]byte, velicina1)
	drugi := make([]byte, velicina2)

	_, _ = summary.Read(prvi)
	_, _ = summary.Read(drugi)

	return &SStableSummary{
		First: string(prvi),
		Last:  string(drugi),
	}
}

func CRC32(str string) uint32 {
	return crc32.ChecksumIEEE([]byte(str))
}

func Get(key string, SummaryFileName string, IndexFileName string, DataFileName string) string {
	sumarryFile, _ := os.OpenFile(SummaryFileName, os.O_RDWR, 0777)
	s := loadSummary(sumarryFile)
	defer sumarryFile.Close()
	fmt.Println("204sstable", s.First, s.Last)
	if s.First <= key && key <= s.Last {
		for {
			v1 := make([]byte, T_SIZE)
			_, err := sumarryFile.Read(v1)
			velicina1 := int64(binary.LittleEndian.Uint64(v1))
			k := make([]byte, velicina1)
			_, err = sumarryFile.Read(k)
			if err != nil {
				panic(err)
			}
			if string(k) == key {
				p := make([]byte, T_SIZE)
				_, err = sumarryFile.Read(p)
				p1 := binary.LittleEndian.Uint64(p)
				dataFilePosition := seekIndex(int64(p1)+int64(len(key)), IndexFileName)
				value := seekData(dataFilePosition, DataFileName)
				sumarryFile.Close()
				return value
			} else {
				p := make([]byte, T_SIZE)
				_, err = sumarryFile.Read(p)
			}
			if err != nil {
				if err == io.EOF {
					sumarryFile.Close()
					break
				}
				fmt.Println(err)
				sumarryFile.Close()
				return ""
			}
		}
	}
	return ""
}

func seekIndex(position int64, IndexFileName string) int64 {
	file, err := os.OpenFile(IndexFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	var offset = position
	var whence = 1
	_, err = file.Seek(offset, whence)
	if err != nil {
		log.Fatal(err)
	}
	bytes := make([]byte, T_SIZE)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(bytes))
}

func seekData(position int64, DataFileName string) string {
	file, err := os.OpenFile(DataFileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	_, err = file.Seek(position, 1)
	if err != nil {
		log.Fatal(err)
	}
	data := make([]byte, 100) // = 41 [44?]
	_, err = file.Read(data)
	if err != nil {
		panic(err)
	}
	key_size := binary.LittleEndian.Uint64(data[TOMBSTONE_SIZE:KEY_SIZE])
	value_size := binary.LittleEndian.Uint64(data[KEY_SIZE:VALUE_SIZE]) //Value size = key + t
	//key_data := string(data[VALUE_SIZE : VALUE_SIZE+key_size])
	val := string(data[VALUE_SIZE+key_size : VALUE_SIZE+key_size+value_size])
	return val
}
