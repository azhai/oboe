package oboe

import "core:c"
import "vendor:zlib"

zlib_compress :: proc(src: []u8, level: c.int = 1) -> ([]u8, bool) {
	if len(src) == 0 {
		return make([]u8, 0), true
	}
	bound := int(zlib.compressBound(u64(len(src))))
	if bound <= 0 {
		return nil, false
	}
	dst := make([]u8, bound)
	dst_len := zlib.uLongf(bound)
	rc := zlib.compress2(raw_data(dst), &dst_len, raw_data(src), u64(len(src)), level)
	if rc != zlib.OK {
		delete(dst)
		return nil, false
	}
	return dst[:int(dst_len)], true
}

zlib_decompress :: proc(src: []u8, raw_len: int) -> ([]u8, bool) {
	if raw_len == 0 {
		return make([]u8, 0), true
	}
	dst := make([]u8, raw_len)
	n := zlib.uLongf(raw_len)
	rc := zlib.uncompress(raw_data(dst), &n, raw_data(src), u64(len(src)))
	if rc != zlib.OK {
		delete(dst)
		return nil, false
	}
	if int(n) != raw_len {
		dst = dst[:int(n)]
	}
	return dst, true
}

// inflate_any auto-detects gzip and zlib streams (windowBits=47).
inflate_any :: proc(src: []u8, max_size: int = 512 << 20) -> ([]u8, bool) {
	if len(src) == 0 {
		return make([]u8, 0), true
	}
	strm: zlib.z_stream
	rc := zlib.inflateInit2(&strm, 47)
	if rc != zlib.OK {
		return nil, false
	}
	defer zlib.inflateEnd(&strm)
	strm.next_in = raw_data(src)
	strm.avail_in = u32(len(src))
	out := make([dynamic]u8, 0, 1 << 16)
	for {
		old := len(out)
		resize(&out, old + (1 << 16))
		strm.next_out = raw_data(out[old:])
		strm.avail_out = 1 << 16
		rc = zlib.inflate(&strm, 0)
		produced := int((1 << 16) - strm.avail_out)
		resize(&out, old + produced)
		if len(out) > max_size {
			delete(out)
			return nil, false
		}
		if rc == zlib.STREAM_END {
			break
		}
		if rc != zlib.OK {
			delete(out)
			return nil, false
		}
		if produced == 0 && strm.avail_in == 0 {
			delete(out)
			return nil, false
		}
	}
	return out[:], true
}
