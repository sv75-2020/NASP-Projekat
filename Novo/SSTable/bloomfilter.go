package SSTable

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type BloomFilter struct {
	elements uint          // broj elemenata
	set      []byte        // Bitset sa elementima
	m        uint          // velicina seta
	k        uint          // broj hes funkcija
	fpr      float64       // False positive
	n        uint          // ocekivani broj item-a
	h        []hash.Hash32 // Hash functions
	ts       int
	name     string
}

func New(n uint) *BloomFilter {
	m := CalculateM(n, 0.01)
	k := CalculateK(n, m)
	ts := int(time.Now().Unix())
	return &BloomFilter{
		n:        n,
		m:        m,
		k:        k,
		fpr:      0.01,
		elements: 0,
		h:        CreateHashFunctions(k, ts),
		set:      make([]byte, m),
		name:     "",
		ts:       ts,
	}
}

func CalculateM(expectedElements uint, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements uint, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint, ts int) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h
}

func findIndex(hfn hash.Hash32, key string, size uint) uint32 {
	hfn.Write([]byte(key))
	idx := hfn.Sum32() % uint32(size)
	hfn.Reset()
	return idx
}

func (bf *BloomFilter) Add(key string, bloom *os.File) {
	for _, hfn := range bf.h {
		index := findIndex(hfn, key, bf.m)
		bf.set[index] = 1
	}
	bf.elements++
	bf.writeBloomFile(bloom)

}

func (bf *BloomFilter) writeBloomFile(bloom *os.File) {
	bloom2, _ := os.OpenFile(bloom.Name(), os.O_WRONLY, 0777)
	bloom2.WriteString(strconv.Itoa(int(bf.elements)) + "|" + string(bf.set) + "|" + strconv.Itoa(int(bf.m)) + "|" + strconv.Itoa(int(bf.k)) + "|" + strconv.Itoa(int(bf.n)) + "|" + strconv.Itoa(bf.ts))
	bloom2.Close()
}

func (bf *BloomFilter) Search(key string) bool {
	for _, hfn := range bf.h {
		idx := findIndex(hfn, key, bf.m)
		if bf.set[idx] != 1 {
			return false
		}
	}
	return true
}

func loadBloom(name string) *BloomFilter {
	content, err := ioutil.ReadFile(name)
	if err != nil {
		return nil
	}
	data := string(content)
	s := strings.Split(data, "|")
	k := StrToUint(s[4])
	ts := StrToUint(s[5])
	return &BloomFilter{
		n:        StrToUint(s[3]),
		m:        StrToUint(s[2]),
		k:        k,
		fpr:      0.01,
		elements: StrToUint(s[0]),
		h:        CreateHashFunctions(k, int(ts)),
		set:      []byte(s[1]),
		name:     name,
		ts:       int(ts),
	}
}

func (bf *BloomFilter) Data() []byte {
	return bf.set
}

func StrToUint(str string) uint {
	num, _ := strconv.ParseUint(str, 10, 64)
	return uint(num)
}

/*func main() {
	bf := New(30)
	bf.Add("Dog")
	bf.Add("Cat")

	fmt.Println("Test Dog [true]:", bf.Search("Dog"))
	fmt.Println("Test Cat [true]:", bf.Search("Cat"))
	fmt.Println("Test John[false]:", bf.Search("John"))
	fmt.Println("Test Doe[false]:", bf.Search("Doe"))

	bf.Add("Pig")
	bf.Add("Mice")
	bf.Add("John")

	fmt.Println("Test Pig [true]:", bf.Search("Pig"))
	fmt.Println("Test Mice [true]:", bf.Search("Mice"))
	fmt.Println("Test John[true]:", bf.Search("John"))
	fmt.Println("Test Mick[false]:", bf.Search("Mick"))

	bf.writeBloomFile()
	newbf := loadBloom()
	fmt.Println(newbf.Data())

}*/
