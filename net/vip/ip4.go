package vip

import (
	"log"
	"net"
	"os"
	"syscall"

	"github.com/mdlayher/arp"
	"github.com/mdlayher/ethernet"
)

var l4 *vipListener4

type vipListener4 struct {
	socket *arpSocket
}

type request4 struct {
	s      *arpSocket
	remote *syscall.SockaddrLinklayer
	frame  *ethernet.Frame
	packet *arp.Packet
}

type arpSocket struct {
	f  *os.File
	rc syscall.RawConn
}

func (r *request4) target() (t net.IP) {
	return r.packet.TargetIP
}

func (r *request4) ifIndex() int {
	return r.remote.Ifindex
}

func (r *request4) reply() (err error) {
	ifc, err := net.InterfaceByIndex(r.remote.Ifindex)
	if err != nil {
		return
	}

	srcIP := r.packet.TargetIP
	srcHW := ifc.HardwareAddr
	dstIP := r.packet.SenderIP
	dstHW := r.packet.SenderHardwareAddr

	p, err := arp.NewPacket(arp.OperationReply, srcHW, srcIP, dstHW, dstIP)
	if err != nil {
		return err
	}
	pb, err := p.MarshalBinary()
	if err != nil {
		return err
	}

	f := &ethernet.Frame{
		Destination: dstHW,
		Source:      srcHW,
		EtherType:   ethernet.EtherTypeARP,
		Payload:     pb,
	}
	fb, err := f.MarshalBinary()
	if err != nil {
		return err
	}

	var baddr [8]byte
	copy(baddr[:], dstHW)

	to := &syscall.SockaddrLinklayer{
		Protocol: r.remote.Protocol,
		Ifindex:  r.remote.Ifindex,
		Hatype:   r.remote.Hatype,
		Pkttype:  r.remote.Pkttype,
		Halen:    r.remote.Halen,
		Addr:     baddr,
	}

	err = r.s.sendTo(fb, 0, to)
	return
}

func newListen4() (l *vipListener4, err error) {
	socket, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, 0x0608)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}

	if err = syscall.SetNonblock(socket, true); err != nil {
		_ = syscall.Close(socket)
		return nil, err
	}
	f := os.NewFile(uintptr(socket), "arp-socket")
	rc, err := f.SyscallConn()
	if err != nil {
		_ = syscall.Close(socket)
		return nil, err
	}
	l = &vipListener4{socket: &arpSocket{f: f, rc: rc}}
	return l, nil
}

func (l *vipListener4) accept() (req vipRequest, err error) {
	buff := make([]byte, buffSize)

	for {
		_, sa, err := l.socket.recvFrom(buff, 0)
		if err != nil {
			return nil, err
		}

		frame := new(ethernet.Frame)
		if err = frame.UnmarshalBinary(buff); err != nil {
			log.Println("unmarshal frame ", err)
			continue
		}

		if frame.EtherType != ethernet.EtherTypeARP {
			continue
		}

		packet := new(arp.Packet)
		if err = packet.UnmarshalBinary(frame.Payload); err != nil {
			log.Println("unmarshal arp failed")
			continue
		}

		if packet.Operation != arp.OperationRequest {
			continue
		}

		remote, ok := sa.(*syscall.SockaddrLinklayer)
		if !ok {
			return nil, syscall.EINVAL
		}

		req = &request4{
			s:      l.socket,
			remote: remote,
			frame:  frame,
			packet: packet,
		}
		//log.Printf("%#v\n", sa)
		return req, nil
	}
}

func (l *vipListener4) gratuitous(ip net.IP) (err error) {
	if len(vipInterfaces) == 0 {
		return
	}

	ifc := vipInterfaces[0]

	srcIP := ip
	srcHW := ifc.HardwareAddr
	dstIP := ip
	dstHW := ethernet.Broadcast

	p, err := arp.NewPacket(arp.OperationReply, srcHW, srcIP, dstHW, dstIP)
	if err != nil {
		return
	}
	pb, err := p.MarshalBinary()
	if err != nil {
		return
	}

	f := &ethernet.Frame{
		Destination: dstHW,
		Source:      srcHW,
		EtherType:   ethernet.EtherTypeARP,
		Payload:     pb,
	}

	fb, err := f.MarshalBinary()
	if err != nil {
		return err
	}

	var baddr [8]byte
	copy(baddr[:], ethernet.Broadcast)
	to := &syscall.SockaddrLinklayer{
		Protocol: 0x0608,
		Ifindex:  ifc.Index,
		Hatype:   0x1,
		Pkttype:  0x0,
		Halen:    0x6,
		Addr:     baddr,
	}
	err = l.socket.sendTo(fb, 0, to)
	return
}

func (as *arpSocket) recvFrom(buff []byte, flags int) (n int, addr syscall.Sockaddr, err error) {
	cerr := as.rc.Read(func(fd uintptr) bool {
		n, addr, err = syscall.Recvfrom(int(fd), buff, flags)
		return err != syscall.EAGAIN
	})

	if err != nil {
		return n, addr, err
	}
	return n, addr, cerr
}

func (as *arpSocket) sendTo(buff []byte, flags int, to syscall.Sockaddr) (err error) {
	cerr := as.rc.Write(func(fd uintptr) bool {
		err = syscall.Sendto(int(fd), buff, flags, to)
		return err != syscall.EAGAIN
	})

	if err != nil {
		return err
	}
	return cerr
}
