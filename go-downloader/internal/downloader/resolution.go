package downloader

import (
	"bytes"
	"encoding/binary"
)

// Resolution 分辨率
type Resolution struct {
	Width  int
	Height int
}

// ReadResolutionFromBytes 从字节流读取分辨率（前 512KB）
func ReadResolutionFromBytes(buf []byte) *Resolution {
	if len(buf) < 10 {
		return nil
	}

	// PNG
	if len(buf) >= 24 && bytes.HasPrefix(buf, []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}) {
		w := int(binary.BigEndian.Uint32(buf[16:20]))
		h := int(binary.BigEndian.Uint32(buf[20:24]))
		return &Resolution{Width: w, Height: h}
	}

	// GIF
	if len(buf) >= 10 && (bytes.HasPrefix(buf, []byte("GIF87a")) || bytes.HasPrefix(buf, []byte("GIF89a"))) {
		w := int(binary.LittleEndian.Uint16(buf[6:8]))
		h := int(binary.LittleEndian.Uint16(buf[8:10]))
		return &Resolution{Width: w, Height: h}
	}

	// WEBP
	if len(buf) >= 30 && bytes.HasPrefix(buf, []byte("RIFF")) && bytes.Equal(buf[8:12], []byte("WEBP")) {
		// VP8X
		idx := bytes.Index(buf, []byte("VP8X"))
		if idx != -1 && len(buf) >= idx+14 {
			payload := buf[idx+4 : idx+14]
			w := 1 + int(Uint24(payload[4:7]))
			h := 1 + int(Uint24(payload[7:10]))
			return &Resolution{Width: w, Height: h}
		}
		// VP8L
		idx = bytes.Index(buf, []byte("VP8L"))
		if idx != -1 && len(buf) >= idx+8 {
			bits := binary.LittleEndian.Uint32(buf[idx+4 : idx+8])
			w := (bits & 0x3FFF) + 1
			h := ((bits >> 14) & 0x3FFF) + 1
			return &Resolution{Width: int(w), Height: int(h)}
		}
		// VP8 (lossy)
		idx = bytes.Index(buf, []byte("VP8 "))
		if idx != -1 {
			payloadOff := idx + 8
			if len(buf) >= payloadOff+10 {
				frameTag := buf[payloadOff : payloadOff+3]
				if (frameTag[0]&0x01) == 0 && bytes.Equal(buf[payloadOff+3:payloadOff+6], []byte{0x9d, 0x01, 0x2a}) {
					wRaw := binary.LittleEndian.Uint16(buf[payloadOff+6 : payloadOff+8])
					hRaw := binary.LittleEndian.Uint16(buf[payloadOff+8 : payloadOff+10])
					w := int(wRaw & 0x3FFF)
					h := int(hRaw & 0x3FFF)
					if w > 0 && h > 0 {
						return &Resolution{Width: w, Height: h}
					}
				}
			}
		}
	}

	// JPEG: 扫描 markers
	if len(buf) >= 2 && buf[0] == 0xFF && buf[1] == 0xD8 {
		i := 2
		n := len(buf)
		for i+1 < n {
			if buf[i] != 0xFF {
				i++
				continue
			}
			for i < n && buf[i] == 0xFF {
				i++
			}
			if i >= n {
				break
			}
			marker := buf[i]
			i++
			if marker == 0xC0 || marker == 0xC2 {
				if i+7 <= n {
					h := int(binary.BigEndian.Uint16(buf[i+3 : i+5]))
					w := int(binary.BigEndian.Uint16(buf[i+5 : i+7]))
					return &Resolution{Width: w, Height: h}
				}
				break
			}
			if i+1 >= n {
				break
			}
			segLen := int(binary.BigEndian.Uint16(buf[i : i+2]))
			if segLen < 2 {
				break
			}
			i += segLen
		}
	}

	return nil
}

// Uint24 读取 24 位无符号整数（little endian）
func Uint24(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}
