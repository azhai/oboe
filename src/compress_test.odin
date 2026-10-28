package oboe

import "core:slice"
import "core:testing"
import "vendor:zlib"

// gzip_test_encode builds a real gzip stream so inflate_any's auto-detection is
// exercised against both zlib and gzip framing.
gzip_test_encode :: proc(src: []u8) -> []u8 {
	strm: zlib.z_stream
	if zlib.deflateInit2(&strm, 1, zlib.DEFLATED, 15 + 16, 8, zlib.DEFAULT_STRATEGY) != zlib.OK {
		return nil
	}
	defer zlib.deflateEnd(&strm)
	cap := int(zlib.compressBound(u64(len(src)))) + 32
	dst := make([]u8, cap)
	strm.next_in = raw_data(src)
	strm.avail_in = u32(len(src))
	strm.next_out = raw_data(dst)
	strm.avail_out = u32(cap)
	if zlib.deflate(&strm, zlib.FINISH) != zlib.STREAM_END {
		delete(dst)
		return nil
	}
	return dst[:int(strm.total_out)]
}

test_payload :: proc() -> Buf {
	b := buf_init(1024)
	for i := 0; i < 20_000; i += 1 {
		buf_byte(&b, u8('a' + i % 26))
	}
	return b
}

@(test)
test_zlib_roundtrip :: proc(t: ^testing.T) {
	payload := test_payload()
	defer buf_destroy(&payload)

	comp, ok := zlib_compress(payload.data[:])
	testing.expect(t, ok)
	if !ok {
		return
	}
	defer delete(comp)
	testing.expect(t, len(comp) < len(payload.data))

	got, dok := zlib_decompress(comp, len(payload.data))
	testing.expect(t, dok)
	if dok {
		defer delete(got)
		testing.expect(t, slice.equal(got, payload.data[:]))
	}

	_, small := zlib_decompress(comp, 16)
	testing.expect(t, !small)
}

@(test)
test_compress_empty_and_invalid :: proc(t: ^testing.T) {
	empty := []u8{}
	comp, ok := zlib_compress(empty)
	testing.expect(t, ok)
	testing.expect_value(t, len(comp), 0)
	delete(comp)

	got, dok := zlib_decompress(empty, 0)
	testing.expect(t, dok)
	testing.expect_value(t, len(got), 0)
	delete(got)

	junk := []u8{1, 2, 3, 4, 5, 6, 7, 8}
	_, bad := zlib_decompress(junk, 64)
	testing.expect(t, !bad)
	_, bad2 := inflate_any(junk)
	testing.expect(t, !bad2)
}

@(test)
test_inflate_any_zlib_and_gzip :: proc(t: ^testing.T) {
	payload := test_payload()
	defer buf_destroy(&payload)

	zcomp, zok := zlib_compress(payload.data[:])
	testing.expect(t, zok)
	if zok {
		defer delete(zcomp)
		zgot, ok := inflate_any(zcomp)
		testing.expect(t, ok)
		if ok {
			defer delete(zgot)
			testing.expect(t, slice.equal(zgot, payload.data[:]))
		}
	}

	gcomp := gzip_test_encode(payload.data[:])
	testing.expect(t, gcomp != nil)
	if gcomp != nil {
		defer delete(gcomp)
		ggot, ok := inflate_any(gcomp)
		testing.expect(t, ok)
		if ok {
			defer delete(ggot)
			testing.expect(t, slice.equal(ggot, payload.data[:]))
		}

		_, too_small := inflate_any(gcomp, 1024)
		testing.expect(t, !too_small)
	}
}
