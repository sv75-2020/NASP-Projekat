package main

import (
	"fmt"
	"time"
)

type TokenBucket struct {
	currentTokens  int   //trenutno koliko se nalazi zahteva u baketu
	maxTokens      int   //koliko maks zahteva moze da primi - OVO U KONFIGURACIONOM FAJLU
	lastTimeFilled int64 //vreme kad je poslednji put punjen
	capacity       int64 //maksimalno vreme - OVO U KONFIGURACIONOM FAJLU
}

func Now() int64 {
	return time.Now().Unix()
}

func (t *TokenBucket) Full() {
	t.currentTokens = t.maxTokens

}

func NewTokenBucket() *TokenBucket {
	return &TokenBucket{
		maxTokens: 5,
		capacity:  int64(60),
	}

}

func (t *TokenBucket) Reset() bool {
	time := Now()
	
	if time-t.lastTimeFilled > t.capacity {
		t.Full()
	}

	if t.currentTokens > 0 {
		t.currentTokens--
		t.lastTimeFilled = time
		return true
	}
	return false

}

func main() {
	n := NewTokenBucket()
	n.Reset()
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
}
