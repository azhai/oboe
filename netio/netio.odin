package netio

import "../common"
import "core:net"
import "core:strconv"
import "core:strings"

// ---------------------------------------------------------------- connection

Conn :: struct {
	sock:  net.TCP_Socket,
	buf:   [1 << 16]u8,
	start: int,
	end:   int,
}

conn_fill :: proc(c: ^Conn) -> bool {
	n, err := net.recv_tcp(c.sock, c.buf[:])
	if err != nil || n <= 0 {
		return false
	}
	c.start = 0
	c.end = n
	return true
}

conn_read_byte :: proc(c: ^Conn) -> (u8, bool) {
	if c.start >= c.end {
		if !conn_fill(c) {
			return 0, false
		}
	}
	b := c.buf[c.start]
	c.start += 1
	return b, true
}

conn_read_exact :: proc(c: ^Conn, n: int, out: ^common.Buf) -> bool {
	remaining := n
	for remaining > 0 {
		if c.start >= c.end {
			if !conn_fill(c) {
				return false
			}
		}
		avail := c.end - c.start
		take := min(avail, remaining)
		common.buf_write(out, c.buf[c.start:c.start+take])
		c.start += take
		remaining -= take
	}
	return true
}

conn_read_line :: proc(c: ^Conn, out: ^common.Buf, max_len: int) -> bool {
	common.buf_reset(out)
	for {
		b, ok := conn_read_byte(c)
		if !ok {
			return false
		}
		if b == '\n' {
			if len(out.data) > 0 && out.data[len(out.data)-1] == '\r' {
				resize(&out.data, len(out.data)-1)
			}
			return true
		}
		common.buf_byte(out, b)
		if len(out.data) > max_len {
			return false
		}
	}
}

// ---------------------------------------------------------------- addressing

parse_listen :: proc(addr: string) -> (ep: net.Endpoint, ok: bool) {
	colon := strings.last_index_byte(addr, ':')
	host := ""
	port_str := addr
	if colon >= 0 {
		host = addr[:colon]
		port_str = addr[colon+1:]
	}
	// Port 0 asks the kernel for a free ephemeral port; callers read the
	// actual port back from the bound socket.
	port, pok := strconv.parse_int(port_str, 10)
	if !pok || port < 0 || port > 65535 {
		return ep, false
	}
	ep.port = port
	if len(host) == 0 || host == "0.0.0.0" || host == "*" {
		ep.address = net.IP4_Address{0, 0, 0, 0}
		return ep, true
	}
	if host == "localhost" {
		ep.address = net.IP4_Address{127, 0, 0, 1}
		return ep, true
	}
	if ip4, iok := net.parse_ip4_address(host); iok {
		ep.address = ip4
		return ep, true
	}
	if ip6, iok := net.parse_ip6_address(host); iok {
		ep.address = ip6
		return ep, true
	}
	return ep, false
}
