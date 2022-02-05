package main

import (
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m   uint64  //velicina seta
	p   uint8   //koliko vodecih bitova koristimo za baket
	reg []uint8 //baketi
}

//ova funkcija racuna velicinu seta preko formule m = 2**p i na taj nacin azurira baket
func NewHLL(p uint8) (*HLL, error) {
	if p < HLL_MIN_PRECISION {
		return nil, errors.New("Preciznost mora biti izmedju 4 i 16")
	}
	if p > HLL_MAX_PRECISION {
		return nil, errors.New("Preciznost mora biti izmedju 4 i 16")
	}
	m := uint64(math.Pow(2, float64(p)))
	return &HLL{p: p, m: m, reg: make([]uint8, m)}, nil
}

//dodaje u odgovarajuci baket
func (hll *HLL) Add(data []byte) {
	x := hll.createHashVal(data)          //pretvaranje u binarne vrednosti
	k := uint32(32 - hll.p)               //kljuc, vrednost baketa
	r := uint8(hll.leftZeros(x << hll.p)) //broj nula
	i := x >> uint8(k)
	if r > hll.reg[i] {
		hll.reg[i] = r
	}
}

//ova funkcija pretvara u binarne vrednosti
func (hll *HLL) createHashVal(b []byte) uint32 {
	h := fnv.New32()
	h.Write(b)
	sum := h.Sum32()
	h.Reset()
	return sum
}

//racuna broj poslednjih nula
func (hll *HLL) leftZeros(x uint32) int {
	return 1 + bits.LeadingZeros32(x)
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
