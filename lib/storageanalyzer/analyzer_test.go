// Copyright 2026 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageanalyzer

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestAnalyzeValidTSSP(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0000001a-0002-000b000f.tssp")
	if err := os.WriteFile(path, buildTSSPFixture(t, "cpu", 136), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(Options{Inputs: []string{path}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.TSSPFiles != 1 {
		t.Fatalf("unexpected tssp count: %d", report.Totals.TSSPFiles)
	}
	if report.Totals.Findings["high"] != 0 {
		t.Fatalf("unexpected high findings: %#v", report.Findings)
	}
	if len(report.Files) != 1 || report.Files[0].Status != "OK" {
		t.Fatalf("unexpected file report: %#v", report.Files)
	}
	trailer := report.Files[0].Metadata["trailer"].(TSSPTrailer)
	if trailer.Measurement != "cpu" {
		t.Fatalf("unexpected measurement: %q", trailer.Measurement)
	}
	if trailer.SectionEnd != 136 {
		t.Fatalf("unexpected section end: %d", trailer.SectionEnd)
	}
}

func TestAnalyzeInvalidTSSPMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000001-0000-00000000.tssp")
	data := buildTSSPFixture(t, "cpu", 136)
	copy(data[:len(tsspMagic)], []byte("badmagic"))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(Options{Inputs: []string{path}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.Findings["high"] == 0 {
		t.Fatalf("expected high finding for invalid magic: %#v", report.Findings)
	}
}

func TestAnalyzeMergeSetPartFindings(t *testing.T) {
	root := t.TempDir()
	part := filepath.Join(root, "10_2_0000000000000001")
	if err := os.MkdirAll(part, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(part, "metadata.json"), []byte(`{"ItemsCount":9,"BlocksCount":2,"FirstItem":"","LastItem":"abcd"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(part, "index.bin"), []byte("idx"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(Options{Inputs: []string{root}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.MergeSetParts != 1 {
		t.Fatalf("unexpected mergeset count: %d", report.Totals.MergeSetParts)
	}
	if report.Totals.Findings["high"] == 0 {
		t.Fatalf("expected high findings: %#v", report.Findings)
	}
}

func buildTSSPFixture(t *testing.T, measurement string, trailerOffset int64) []byte {
	t.Helper()
	trailer := make([]byte, 0, 256)
	trailer = appendInt64(trailer, 16)
	trailer = appendInt64(trailer, 64)
	trailer = appendInt64(trailer, 32)
	trailer = appendInt64(trailer, 16)
	trailer = appendInt64(trailer, 8)
	trailer = appendInt64(trailer, 0)
	trailer = appendInt64(trailer, 2)
	trailer = binary.BigEndian.AppendUint64(trailer, 10)
	trailer = binary.BigEndian.AppendUint64(trailer, 20)
	trailer = appendInt64(trailer, 100)
	trailer = appendInt64(trailer, 200)
	trailer = appendInt64(trailer, 1)
	trailer = binary.BigEndian.AppendUint64(trailer, 64)
	trailer = binary.BigEndian.AppendUint64(trailer, 3)
	trailer = binary.BigEndian.AppendUint16(trailer, 8)
	flags := uint64(1) | uint64(10)<<32
	trailer = binary.LittleEndian.AppendUint64(trailer, flags)
	trailer = binary.BigEndian.AppendUint16(trailer, 0)
	trailer = binary.BigEndian.AppendUint16(trailer, uint16(len(measurement)))
	trailer = append(trailer, measurement...)

	data := make([]byte, trailerOffset)
	copy(data[:len(tsspMagic)], []byte(tsspMagic))
	binary.BigEndian.PutUint64(data[len(tsspMagic):tsspHeaderSize], 2)
	data = append(data, trailer...)
	data = appendInt64(data, trailerOffset)
	return data
}

func appendInt64(dst []byte, value int64) []byte {
	encoded := uint64((value << 1) ^ (value >> 63))
	return binary.BigEndian.AppendUint64(dst, encoded)
}
