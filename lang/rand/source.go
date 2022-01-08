package rand

import (
	"math/rand"
	"sync"
	"time"
)

var (
	r *rand.Rand
)

func init() {
	src := rand.NewSource(time.Now().UnixNano()).(rand.Source64)
	r = rand.New(&lockedSource{src: src})
}

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source64
}

func (r *lockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Uint64() (n uint64) {
	r.lk.Lock()
	n = r.src.Uint64()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}
