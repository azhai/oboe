package netio

import "core:net"
import "core:testing"

@(test)
test_parse_listen :: proc(t: ^testing.T) {
	ep, ok := parse_listen("127.0.0.1:9428")
	testing.expect(t, ok)
	testing.expect_value(t, ep.port, 9428)
	if ip4, is_ip4 := ep.address.(net.IP4_Address); is_ip4 {
		testing.expect_value(t, ip4, net.IP4_Address{127, 0, 0, 1})
	} else {
		testing.expect(t, false)
	}

	ep, ok = parse_listen(":8080")
	testing.expect(t, ok)
	testing.expect_value(t, ep.port, 8080)
	if ip4, is_ip4 := ep.address.(net.IP4_Address); is_ip4 {
		testing.expect_value(t, ip4, net.IP4_Address{0, 0, 0, 0})
	} else {
		testing.expect(t, false)
	}

	ep, ok = parse_listen("localhost:80")
	testing.expect(t, ok)
	testing.expect_value(t, ep.port, 80)
	if ip4, is_ip4 := ep.address.(net.IP4_Address); is_ip4 {
		testing.expect_value(t, ip4, net.IP4_Address{127, 0, 0, 1})
	} else {
		testing.expect(t, false)
	}

	_, ok = parse_listen("0.0.0.0:1")
	testing.expect(t, ok)

	// Port 0 is valid: the kernel picks an ephemeral port.
	ep, ok = parse_listen("127.0.0.1:0")
	testing.expect(t, ok)
	testing.expect_value(t, ep.port, 0)

	bad_addrs := []string{"", "nope", "1.2.3.4", "1.2.3.4:-1", "1.2.3.4:70000", "1.2.3.4:x"}
	for bad in bad_addrs {
		_, ok = parse_listen(bad)
		testing.expectf(t, !ok, "parse_listen(%q) should fail", bad)
	}
}
