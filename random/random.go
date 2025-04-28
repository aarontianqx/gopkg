package random

import (
	"math/rand/v2"

	"golang.org/x/exp/constraints"
)

// IntInRange returns a random integer in range [min, max).
// Returns min if max <= min.
func IntInRange[T constraints.Integer](min, max T) T {
	if max <= min {
		return min
	}
	return min + T(rand.Int64N(int64(max-min)))
}

// KeysFromMap randomly selects k keys from the provided map.
// Returns all keys if k >= map size, empty slice if k <= 0.
// Uses Fisher-Yates shuffle algorithm for random selection.
func KeysFromMap[K comparable, V any](m map[K]V, k int) []K {
	if k <= 0 {
		return []K{}
	}

	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	if k >= len(keys) {
		return keys
	}

	for i := len(keys) - 1; i > len(keys)-k-1; i-- {
		j := rand.IntN(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}

	return keys[len(keys)-k:]
}

// ProbabilityCheck returns true with the given probability [0.0,1.0].
// Always returns false if probability <= 0.0, true if probability >= 1.0.
func ProbabilityCheck[T constraints.Float](probability T) bool {
	if probability <= T(0.0) {
		return false
	}
	if probability >= T(1.0) {
		return true
	}
	return rand.Float64() < float64(probability)
}

// AlphanumericString generates a random string of length n containing
// alphanumeric characters (0-9, a-z, A-Z).
func AlphanumericString(n int) string {
	const alphaDigits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const letterCount = int64(len(alphaDigits))
	const letterIdxBits = 6 // 2^6 = 64 > letterCount
	const letterIdxMask = 1<<letterIdxBits - 1
	const letterIdxMax = letterIdxMask / letterIdxBits

	letterBytes := []byte(alphaDigits)
	b := make([]byte, n)

	for i, cache, remain := n-1, rand.Int64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int64(), letterIdxMax
		}
		if idx := cache & letterIdxMask; idx < letterCount {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
