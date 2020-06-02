package ndproxy

import (
	"fmt"
	"log"
	"net"
	"sync"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv6"
)

const (
	ipProtocolICMP6 = 58
)

var (
	_proxy *ndProxy
)

type ndProxy struct {
	services map[string]*ndService
	lock     sync.Mutex
}

func init() {
	_proxy = &ndProxy{
		services: make(map[string]*ndService),
	}
}

type ndService struct {
	conn    *ipv6.PacketConn
	address string
	closed  bool
}

func (s *ndService) close() {
	s.closed = true
	_ = s.conn.Close()
}

func AddAddress(address string) (err error) {
	addr, err := net.ResolveIPAddr("ip6", address)
	if err != nil {
		return
	}

	if !addr.IP.IsGlobalUnicast() || addr.IP.IsLoopback() {
		return fmt.Errorf("%s not an unicast address", addr)
	}

	var exist bool
	_proxy.lock.Lock()
	_, exist = _proxy.services[addr.String()]
	if exist {
		_proxy.lock.Unlock()
		return
	}
	defer _proxy.lock.Unlock()

	s, err := newService(addr)
	if err != nil {
		return
	}
	_proxy.services[addr.String()] = s
	go s.start()
	return
}

func DelAddress(addr string) {
	_proxy.lock.Lock()
	defer _proxy.lock.Unlock()

	s, exist := _proxy.services[addr]
	if exist {
		delete(_proxy.services, addr)
		s.close()
	}
}

func newService(addr *net.IPAddr) (s *ndService, err error) {
	var maddr net.IP

	maddr = net.IP{0xff, 0x02, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0xff, addr.IP[13], addr.IP[14], addr.IP[15]}

	group, err := net.ResolveIPAddr("ip6", maddr.String())
	if err != nil {
		return
	}

	c, err := icmp.ListenPacket("ip6:ipv6-icmp", addr.String())
	if err != nil {
		return
	}

	conn := c.IPv6PacketConn()
	var f ipv6.ICMPFilter
	f.SetAll(true)
	f.Accept(ipv6.ICMPTypeNeighborSolicitation)
	if err = conn.SetICMPFilter(&f); err != nil {
		_ = conn.Close()
		return
	}
	if err = conn.SetChecksum(true, 2); err != nil {
		_ = conn.Close()
		return
	}
	if err = conn.SetControlMessage(ipv6.FlagSrc|ipv6.FlagDst|ipv6.FlagInterface, true); err != nil {
		_ = conn.Close()
		return
	}
	if err = conn.SetHopLimit(255); err != nil {
		_ = conn.Close()
		return
	}
	if err = conn.JoinGroup(nil, group); err != nil {
		_ = conn.Close()
		return
	}

	s = &ndService{
		conn:    conn,
		address: addr.String(),
	}
	return
}

func (s *ndService) start() {
	for !s.closed {
		buff := make([]byte, 2048)
		_, cm, src, err := s.conn.ReadFrom(buff)
		if err != nil {
			log.Println(err)
			continue
		}

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

		var target net.IP
		target = buff[8:24]

		if target.String() != s.address {
			continue
		}

		sendNeighborAdvertisement(s.conn, cm, src, target)
	}
}

func sendNeighborAdvertisement(conn *ipv6.PacketConn, cm *ipv6.ControlMessage, src net.Addr, target net.IP) {
	ifc, err := net.InterfaceByIndex(cm.IfIndex)
	if err != nil {
		log.Println(err)
		return
	}

	ad := newAdvertisement(cm.Src, target, ifc.HardwareAddr)
	data := ad.marshal()
	_, err = conn.WriteTo(data, nil, src)
	if err != nil {
		log.Println(err)
	}
}
