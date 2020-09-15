package vip

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
)

const (
	buffSize = 1024
)

type virtualIpAddress struct {
	address string
	isIp6   bool
	ip      net.IP
}

type vipMap struct {
	addresses map[string]*virtualIpAddress
	lock      sync.Mutex
}

type vipRequest interface {
	target() net.IP
	reply() error
	ifIndex() int
}

type vipListener interface {
	accept() (vipRequest, error)
}

var (
	vipes         vipMap
	vipInterfaces []*net.Interface
)

func init() {
	vipes = vipMap{
		addresses: make(map[string]*virtualIpAddress),
	}
	vipInterfaces = make([]*net.Interface, 0, 8)
}

func VipInterface(name string) (err error) {
	ifc, err := net.InterfaceByName(name)
	if err != nil {
		return
	}

	for _, i := range vipInterfaces {
		if i.Name == strings.TrimSpace(name) {
			return
		}
	}

	vipInterfaces = append(vipInterfaces, ifc)
	return
}

//Add vip to loopback interface
func Add(address string) (err error) {
	if err = checkInit(); err != nil {
		return err
	}

	v, err := parseIP(address)
	if err != nil {
		return err
	}
	setLookup(v)
	return
}

//Delete vip from loopback interface
func Delete(address string) (err error) {
	if err = checkInit(); err != nil {
		return err
	}

	v, err := parseIP(address)
	if err != nil {
		return err
	}
	_ = Delete(v.address)
	unsetLookup(v)
	return
}

//Enable vip
func Enable(address string) (err error) {
	if err = checkInit(); err != nil {
		return err
	}

	v, err := parseIP(address)
	if err != nil {
		return err
	}
	vipes.add(v)
	if v.isIp6 {
		l6.joinGroup(v.ip)
		l6.gratuitous(v.ip)
	} else {
		err = l4.gratuitous(v.ip)
	}
	return
}

//Disable vip
func Disable(address string) (err error) {
	if err = checkInit(); err != nil {
		return err
	}

	v, err := parseIP(address)
	if err != nil {
		return
	}
	vipes.del(v.address)
	if v.isIp6 {
		l6.leaveGroup(v.ip)
	}
	return
}

func (vmap *vipMap) add(addr *virtualIpAddress) {
	vmap.lock.Lock()
	defer vmap.lock.Unlock()
	vmap.addresses[addr.address] = addr
}

func (vmap *vipMap) del(addr string) {
	vmap.lock.Lock()
	defer vmap.lock.Unlock()
	delete(vmap.addresses, addr)
}

func (vmap *vipMap) get(addr string) (va *virtualIpAddress) {
	vmap.lock.Lock()
	defer vmap.lock.Unlock()
	va, _ = vmap.addresses[addr]
	return
}

func parseIP(addr string) (vip *virtualIpAddress, err error) {
	ip := net.ParseIP(addr)
	if len(ip) != net.IPv4len && len(ip) != net.IPv6len {
		return nil, net.InvalidAddrError(addr)
	}

	if ip.IsLoopback() {
		return nil, net.InvalidAddrError(addr)
	}

	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
	}

	vip = &virtualIpAddress{
		address: ip.String(),
		isIp6:   len(ip) == net.IPv6len,
		ip:      ip,
	}
	return
}

func startListen(l vipListener) {
	for {
		req, err := l.accept()
		if err != nil {
			continue
		}
		if vipes.get(req.target().String()) != nil {
			if len(vipInterfaces) == 0 {
				_ = req.reply()
				continue
			}

			for _, ifc := range vipInterfaces {
				if ifc.Index == req.ifIndex() {
					_ = req.reply()
				}
			}
		}
	}
}

func setLookup(v *virtualIpAddress) {
	addr := fmt.Sprintf("%s/%d", v.ip, len(v.ip)*8)
	args := []string{"address", "add", addr, "dev", "lo"}
	cmd := exec.Command("/usr/sbin/ip", args...)
	_ = cmd.Run()
}
func unsetLookup(v *virtualIpAddress) {
	addr := fmt.Sprintf("%s/%d", v.ip, len(v.ip)*8)
	args := []string{"address", "del", addr, "dev", "lo"}
	cmd := exec.Command("/usr/sbin/ip", args...)
	_ = cmd.Run()
}

var initLock sync.Mutex

func checkInit() (err error) {
	initLock.Lock()
	defer initLock.Unlock()
	return initResource()
}

func initResource() (err error) {
	if l6 == nil {
		l6, err = createListen6()
		if err != nil {
			return
		}
		go startListen(l6)
	}

	if l4 == nil {
		l4, err = newListen4()
		if err != nil {
			return
		}
		go startListen(l4)
	}
	return
}
