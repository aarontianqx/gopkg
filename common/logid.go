package common

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

const (
	version    = "02"
	length     = 29 // 53 if ipv6 used
	maxRandNum = 1<<24 - 1<<20
)

func GenLogID() string {
	r := r32(maxRandNum) + 1<<20
	sb := strings.Builder{}
	sb.Grow(length)
	// 2bit
	sb.WriteString(version) // 2bit
	// 13bit
	sb.WriteString(strconv.FormatUint(uint64(time.Now().UnixMilli()), 10))
	// trunk 8 bit from 32 bit as ipv4 isn't that long
	sb.Write(GetHexLocalIP()[24:])
	// 6 bit
	sb.WriteString(strconv.FormatUint(uint64(r), 16))
	return sb.String()
}

func r32(n uint32) uint32 {
	return uint32(uint64(rand.Uint32()) * uint64(n) >> 32)
}
