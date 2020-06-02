package ndproxy

import (
	"encoding/binary"
	"net"

	"golang.org/x/net/ipv6"
)

const (
	neighborAdvertisementFlags = 0x60000000
	advertisementSize          = 32
)

type advertisement struct {
	Type                  uint8  // 1 byte
	Code                  uint8  // 1 byte
	CheckSum              uint16 // 2 bytes
	Flags                 uint32 // 4 bytes
	TargetAddress         net.IP // 16 bytes
	OptType               uint8  // 1 byte
	OptLength             uint8  // 1 byte
	OptLinkerLayerAddress []byte // 6 bytes
	src                   net.IP
}

func newAdvertisement(src, target net.IP, mac []byte) (ad *advertisement) {
	ad = &advertisement{
		Type:                  uint8(ipv6.ICMPTypeNeighborAdvertisement),
		Flags:                 neighborAdvertisementFlags,
		TargetAddress:         target,
		OptType:               2,
		OptLength:             1,
		OptLinkerLayerAddress: mac,
		src:                   src,
	}
	return
}

func (ad *advertisement) marshal() (data []byte) {
	payload := make([]byte, advertisementSize)
	payload[0] = ad.Type
	payload[1] = ad.Code
	binary.BigEndian.PutUint16(payload[2:4], ad.CheckSum)
	binary.BigEndian.PutUint32(payload[4:8], ad.Flags)
	for i := 0; i < 16; i++ {
		payload[8+i] = ad.TargetAddress[i]
	}

	payload[24] = ad.OptType
	payload[25] = ad.OptLength
	for i := 0; i < 6; i++ {
		payload[26+i] = ad.OptLinkerLayerAddress[i]
	}
	return payload
}
