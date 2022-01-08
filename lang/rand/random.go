package rand

import (
	"unsafe"
)

const (
	alphaDigits   = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterCount   = int64(len(alphaDigits))
	letterIdxBits = 6 // 2 ^ 6 = 64 > letterCount
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = letterIdxMask / letterIdxBits
)

var (
	letterBytes = []byte(alphaDigits)
)

func Seed(seed int64) { r.Seed(seed) }

func Int63() int64 { return r.Int63() }

func Uint32() uint32 { return r.Uint32() }

func Uint64() uint64 { return r.Uint64() }

func Int31() int32 { return r.Int31() }

func Int() int { return r.Int() }

func Int63n(n int64) int64 { return r.Int63n(n) }

func Int31n(n int32) int32 { return r.Int31n(n) }

func Intn(n int) int { return r.Intn(n) }

func Float64() float64 { return r.Float64() }

func Float32() float32 { return r.Float32() }

func Perm(n int) []int { return r.Perm(n) }

func Shuffle(n int, swap func(i, j int)) { r.Shuffle(n, swap) }

func Read(p []byte) (n int, err error) { return r.Read(p) }

func NormFloat64() float64 { return r.NormFloat64() }

func ExpFloat64() float64 { return r.ExpFloat64() }

//func AlphaDigitsByBytes(n int) string {
//	b := make([]byte, n)
//	for i := range b {
//		b[i] = letterBytes[r.Int63()%letterCount]
//	}
//	return *(*string)(unsafe.Pointer(&b))
//}

func AlphaDigits(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, r.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = r.Int63(), letterIdxMax
		}
		if idx := cache & letterIdxMask; idx < letterCount {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}
