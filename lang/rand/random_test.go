package rand

import "testing"

//func BenchmarkRandomAlphaDigitsByBytes(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		_ = AlphaDigitsByBytes(10)
//	}
//}

func BenchmarkRandomAlphaDigits(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = AlphaDigits(10)
	}
}
