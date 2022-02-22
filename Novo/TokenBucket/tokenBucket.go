package TokenBucket

import (
	config "Novo/Config"
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
	c := config.Config{}
	c.ReadConfigFile()
	return &TokenBucket{
		maxTokens: c.GetMaxTokens(),
		capacity:  c.GetMaxTime(),
	}

}

func (t *TokenBucket) Reset() bool {
	now := Now()

	if now-t.lastTimeFilled > t.capacity {
		t.Full()
	}

	if t.currentTokens > 0 {
		t.currentTokens--
		t.lastTimeFilled = now
		return true
	}
	return false

}

/*func main() {
	n := NewTokenBucket()
	n.Reset()
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
	fmt.Println(n.Reset())
}*/
