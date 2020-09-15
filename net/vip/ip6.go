package vip

import (
	"encoding/binary"
	"log"
	"net"
	"sync"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv6"
)

const (
	ipProtocolICMP6            = 58
	neighborAdvertisementFlags = 0x60000000
	advertisementSize          = 32
)

var (
	l6 *listener6
)

func composeGroupAddress(ip net.IP) (group net.IP) {
	return net.IP{0xff, 0x02, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0xff, ip[13], ip[14], ip[15]}
}

type listener6 struct {
	conn *ipv6.PacketConn
	gm   *groupMap
}

type request6 struct {
	conn   *ipv6.PacketConn
	cm     *ipv6.ControlMessage
	tgt    net.IP
	remote net.Addr
}

func (r *request6) target() (ip net.IP) {
	return r.tgt
}

func (r *request6) ifIndex() int {
	return r.cm.IfIndex
}

type advertisement struct {
	Type                  uint8  // 1 byte
	Code                  uint8  // 1 byte
	CheckSum              uint16 // 2 bytes
	Flags                 uint32 // 4 bytes
	TargetAddress         net.IP // 16 bytes
	OptType               uint8  // 1 byte
	OptLength             uint8  // 1 byte
	OptLinkerLayerAddress []byte // 6 bytes
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

func (r *request6) reply() (err error) {
	ifc, err := net.InterfaceByIndex(r.cm.IfIndex)
	if err != nil {
		return
	}

	ad := &advertisement{
		Type:                  uint8(ipv6.ICMPTypeNeighborAdvertisement),
		Flags:                 neighborAdvertisementFlags,
		TargetAddress:         r.tgt,
		OptType:               2,
		OptLength:             1,
		OptLinkerLayerAddress: ifc.HardwareAddr,
	}

	cm := &ipv6.ControlMessage{
		Src:     r.tgt,
		IfIndex: ifc.Index,
	}

	data := ad.marshal()
	_, err = r.conn.WriteTo(data, cm, r.remote)
	return
}

func (l *listener6) gratuitous(ip net.IP) {
	if len(vipInterfaces) == 0 {
		return
	}

	ifc := vipInterfaces[0]

	g := composeGroupAddress(ip)
	dst, _ := net.ResolveIPAddr("ip6", g.String())
	cm := &ipv6.ControlMessage{
		Src:     ip,
		IfIndex: ifc.Index,
	}

	ns := &advertisement{
		Type:                  uint8(ipv6.ICMPTypeNeighborAdvertisement),
		Flags:                 neighborAdvertisementFlags,
		TargetAddress:         ip,
		OptType:               2,
		OptLength:             1,
		OptLinkerLayerAddress: ifc.HardwareAddr,
	}
	data := ns.marshal()
	_, _ = l.conn.WriteTo(data, cm, dst)
}

func createListen6() (l *listener6, err error) {
	var conn *icmp.PacketConn
	var filter ipv6.ICMPFilter

	if conn, err = icmp.ListenPacket("ip6:ipv6-icmp", "::"); err != nil {
		return
	}
	conn6 := conn.IPv6PacketConn()
	filter.SetAll(true)
	filter.Accept(ipv6.ICMPTypeNeighborSolicitation)
	if err = conn6.SetICMPFilter(&filter); err != nil {
		_ = conn6.Close()
		return
	}

	if err = conn6.SetChecksum(true, 2); err != nil {
		_ = conn6.Close()
		return
	}

	if err = conn6.SetControlMessage(ipv6.FlagSrc|ipv6.FlagDst|ipv6.FlagInterface, true); err != nil {
		_ = conn6.Close()
		return
	}

	if err = conn6.SetHopLimit(255); err != nil {
		_ = conn6.Close()
		return
	}

	l = &listener6{
		conn: conn6,
	}
	l.gm = &groupMap{
		groups: make(map[string][]string),
	}
	return
}

func (l *listener6) accept() (req vipRequest, err error) {
	for {
		buff := make([]byte, buffSize)
		n, cm, remote, err := l.conn.ReadFrom(buff)
		if err != nil {
			log.Println(err)
			continue
		}
		buff = buff[:n]
		if len(buff) < 24 {
			continue
		}

		msg, err := icmp.ParseMessage(ipProtocolICMP6, buff)
		if err != nil {
			continue
		}

		if msg.Type != ipv6.ICMPTypeNeighborSolicitation {
			continue
		}

		target := make(net.IP, net.IPv6len)
		copy(target, buff[8:24])

		req = &request6{
			conn:   l.conn,
			cm:     cm,
			remote: remote,
			tgt:    target,
		}
		return req, nil
	}
}

func (l *listener6) joinGroup(ip6 net.IP) {
	op := l.gm.joinGroup(ip6)
	if op == groupNoOperation {
		return
	}

	ip := composeGroupAddress(ip6)
	g, err := net.ResolveIPAddr("ip6", ip.String())
	if err != nil {
		return
	}
	var ifc *net.Interface = nil
	if len(vipInterfaces) != 0 {
		ifc = vipInterfaces[0]
	}

	_ = l.conn.JoinGroup(ifc, g)
}

func (l *listener6) leaveGroup(ip6 net.IP) {
	op := l.gm.leaveGroup(ip6)
	if op == groupNoOperation {
		return
	}
	ip := composeGroupAddress(ip6)
	g, err := net.ResolveIPAddr("ip6", ip.String())
	if err != nil {
		return
	}

	var ifc *net.Interface = nil
	if len(vipInterfaces) != 0 {
		ifc = vipInterfaces[0]
	}
	_ = l.conn.LeaveGroup(ifc, g)
}

const (
	groupNoOperation = iota
	groupAdd
	groupDelete
)

type groupMap struct {
	groups map[string][]string
	lock   sync.Mutex
}

func (gm *groupMap) joinGroup(ip6 net.IP) int {
	group := composeGroupAddress(ip6)
	gm.lock.Lock()
	defer gm.lock.Unlock()

	addr := gm.groups[group.String()]
	if addr == nil {
		addr = make([]string, 8)
		addr[0] = ip6.String()
		return groupAdd
	}

	for _, a := range addr {
		if a == ip6.String() {
			return groupNoOperation
		}
	}

	addr = append(addr, ip6.String())
	return groupNoOperation
}

func (gm *groupMap) leaveGroup(ip6 net.IP) int {
	group := composeGroupAddress(ip6)
	gm.lock.Lock()
	defer gm.lock.Unlock()

	addr, ok := gm.groups[group.String()]
	if !ok {
		return groupNoOperation
	}

	if addr == nil {
		delete(gm.groups, ip6.String())
		return groupDelete
	}

	exist := -1

	for i := 0; i < len(addr); i++ {
		a := addr[i]
		if a == ip6.String() {
			exist = i
			break
		}
	}

	if exist == -1 {
		return groupNoOperation
	}

	addr = remove(addr, exist)

	if len(addr) == 0 {
		delete(gm.groups, group.String())
		return groupDelete
	}

	gm.groups[group.String()] = addr
	return groupNoOperation
}

func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
