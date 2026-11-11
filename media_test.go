package main

import (
	"bytes"
	"testing"
)

func TestH264StreamSplitsAccessUnits(t *testing.T) {
	var raw []byte
	raw = append(raw, 0, 0, 0, 1, 0x67, 0xaa)
	raw = append(raw, 0, 0, 0, 1, 0x68, 0xbb)
	raw = append(raw, 0, 0, 0, 1, 0x65, 0xcc)
	raw = append(raw, 0, 0, 0, 1, 0x61, 0xdd)
	raw = append(raw, 0, 0, 0, 1, 0x61, 0xee)
	raw = append(raw, 0, 0, 0, 1)

	stream := &h264Stream{}
	var got [][]byte
	for i := 0; i < len(raw); i += 3 {
		end := i + 3
		if end > len(raw) {
			end = len(raw)
		}
		got = append(got, stream.Push(raw[i:end])...)
	}
	if len(got) != 3 {
		t.Fatalf("got %d access units, want 3", len(got))
	}
	if !bytes.Contains(got[0], []byte{0x67, 0xaa}) || !bytes.Contains(got[0], []byte{0x65, 0xcc}) {
		t.Fatalf("first unit = %x, want SPS and IDR together", got[0])
	}
	if bytes.Contains(got[1], []byte{0x67, 0xaa}) || !bytes.Contains(got[1], []byte{0x61, 0xdd}) {
		t.Fatalf("second unit = %x, want one P frame", got[1])
	}
}
