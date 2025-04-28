package random

import (
	"math"
	"testing"
)

func TestIntInRange(t *testing.T) {
	tests := []struct {
		name    string
		min     int
		max     int
		wantMin int
		wantMax int
	}{
		{"Valid range", 5, 10, 5, 10},
		{"Empty range", 5, 5, 5, 5},
		{"Invalid range", 10, 5, 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				result := IntInRange(tt.min, tt.max)
				if result < tt.wantMin || result >= tt.wantMax {
					if tt.wantMin != tt.wantMax {
						t.Errorf("IntInRange(%d, %d) = %d, want range [%d, %d)",
							tt.min, tt.max, result, tt.wantMin, tt.wantMax)
					} else if result != tt.wantMin {
						t.Errorf("IntInRange(%d, %d) = %d, want %d",
							tt.min, tt.max, result, tt.wantMin)
					}
				}
			}
		})
	}

	// Test with different integer types
	t.Run("Different integer types", func(t *testing.T) {
		// int8
		result8 := IntInRange(int8(5), int8(10))
		if result8 < 5 || result8 >= 10 {
			t.Errorf("IntInRange(int8(5), int8(10)) = %d, want range [5, 10)", result8)
		}

		// uint
		resultUint := IntInRange(uint(5), uint(10))
		if resultUint < 5 || resultUint >= 10 {
			t.Errorf("IntInRange(uint(5), uint(10)) = %d, want range [5, 10)", resultUint)
		}
	})
}

func TestKeysFromMap(t *testing.T) {
	testMap := map[string]int{
		"a": 1, "b": 2, "c": 3, "d": 4, "e": 5,
	}

	tests := []struct {
		name      string
		m         map[string]int
		k         int
		wantCount int
	}{
		{"Select subset", testMap, 3, 3},
		{"Select all", testMap, 5, 5},
		{"Select more than size", testMap, 10, 5},
		{"Select zero", testMap, 0, 0},
		{"Select negative", testMap, -1, 0},
		{"Empty map", map[string]int{}, 3, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := KeysFromMap(tt.m, tt.k)
			if len(result) != tt.wantCount {
				t.Errorf("KeysFromMap() returned %d keys, want %d", len(result), tt.wantCount)
			}

			// Check that all selected keys are unique and from the original map
			seen := make(map[string]bool)
			for _, key := range result {
				if seen[key] {
					t.Errorf("KeysFromMap() returned duplicate key: %v", key)
				}
				seen[key] = true

				if _, exists := tt.m[key]; !exists {
					t.Errorf("KeysFromMap() returned key %v not in original map", key)
				}
			}
		})
	}
}

func TestProbabilityCheck(t *testing.T) {
	tests := []struct {
		name        string
		probability float64
		wantTrue    bool
		iterations  int
	}{
		{"Always false", 0.0, false, 1},
		{"Always true", 1.0, true, 1},
		{"Always false negative", -0.1, false, 1},
		{"Always true high", 1.1, true, 1},
		{"50% probability", 0.5, true, 1000}, // Just run multiple times, can't test exact distribution
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.probability {
			case 0.0, -0.1:
				// Should always be false
				if ProbabilityCheck(tt.probability) != false {
					t.Errorf("ProbabilityCheck(%f) = true, want false", tt.probability)
				}
			case 1.0, 1.1:
				// Should always be true
				if ProbabilityCheck(tt.probability) != true {
					t.Errorf("ProbabilityCheck(%f) = false, want true", tt.probability)
				}
			case 0.5:
				// Test statistical distribution
				trueCount := 0
				for i := 0; i < tt.iterations; i++ {
					if ProbabilityCheck(tt.probability) {
						trueCount++
					}
				}
				ratio := float64(trueCount) / float64(tt.iterations)
				// Allow for some statistical variance, within 3 standard deviations
				expectedRatio := tt.probability
				stdDev := math.Sqrt(expectedRatio * (1 - expectedRatio) / float64(tt.iterations))
				if math.Abs(ratio-expectedRatio) > 3*stdDev {
					t.Errorf("ProbabilityCheck(%f) distribution = %f, want approximately %f (outside 3σ)",
						tt.probability, ratio, expectedRatio)
				}
			}
		})
	}

	// Test with float32
	t.Run("float32 type", func(t *testing.T) {
		if ProbabilityCheck(float32(0.0)) != false {
			t.Errorf("ProbabilityCheck(float32(0.0)) = true, want false")
		}
		if ProbabilityCheck(float32(1.0)) != true {
			t.Errorf("ProbabilityCheck(float32(1.0)) = false, want true")
		}
	})
}

func TestAlphanumericString(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"Zero length", 0},
		{"Small string", 5},
		{"Medium string", 20},
		{"Large string", 100},
	}

	const validChars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	isValidChar := func(c byte) bool {
		for i := 0; i < len(validChars); i++ {
			if validChars[i] == c {
				return true
			}
		}
		return false
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AlphanumericString(tt.length)

			// Check length
			if len(result) != tt.length {
				t.Errorf("AlphanumericString(%d) returned string of length %d, want %d",
					tt.length, len(result), tt.length)
			}

			// Check all characters are valid
			for i := 0; i < len(result); i++ {
				if !isValidChar(result[i]) {
					t.Errorf("AlphanumericString(%d) contains invalid character %c at position %d",
						tt.length, result[i], i)
				}
			}

			// Test randomness by checking that multiple calls produce different results
			// Skip for zero length
			if tt.length > 0 {
				anotherResult := AlphanumericString(tt.length)
				if result == anotherResult {
					// There's a small chance this could happen randomly,
					// so try one more time before failing
					yetAnotherResult := AlphanumericString(tt.length)
					if result == yetAnotherResult && result == anotherResult {
						t.Errorf("AlphanumericString(%d) produced identical strings across multiple calls",
							tt.length)
					}
				}
			}
		})
	}
}
