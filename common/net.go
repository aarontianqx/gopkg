package common

import (
	"encoding/hex"
	"net"
)

const (
	unknownIP = "00000000000000000000000000000000"
)

var (
	localIP    net.IP
	hexLocalIP []byte
)

func init() {
	localIP = getOutboundIP()
	hexLocalIP = getHexIP(localIP)
}

// getOutboundIP Get preferred outbound ip of this machine
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func getHexIP(ip net.IP) []byte {
	src := ip.To16()
	if src == nil {
		return []byte(unknownIP)
	}
	dst := make([]byte, 32)
	hex.Encode(dst, localIP.To16())
	return dst
}

func GetLocalIP() net.IP {
	return localIP
}

func GetHexLocalIP() []byte {
	return hexLocalIP
}
