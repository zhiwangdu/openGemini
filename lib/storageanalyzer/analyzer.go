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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	ToolName            = "opengemini_storage_analyzer"
	defaultMaxFiles     = 512
	tsspMagic           = "53ac2021"
	tsspHeaderSize      = len(tsspMagic) + 8
	tsspFooterSize      = 8
	maxTrailerReadBytes = 8 * 1024 * 1024
)

var (
	tsspNameRE        = regexp.MustCompile(`^([0-9a-f]{8,16})-([0-9a-f]{4})-([0-9a-f]{4})([0-9a-f]{4})\.tssp(\.init)?$`)
	mergesetPartRE    = regexp.MustCompile(`^([0-9]+)_([0-9]+)_([^/]+)$`)
	bloomFilterFileRE = regexp.MustCompile(`^[0-9]+_mergeset\.bf(\.last|\.init)?$`)
	errMaxFiles       = errors.New("maximum file count reached")
)

type Options struct {
	Inputs   []string
	MaxFiles int
}

type Report struct {
	SchemaVersion int          `json:"schemaVersion"`
	Tool          string       `json:"tool"`
	Summary       string       `json:"summary"`
	Findings      []Finding    `json:"findings"`
	Inputs        []string     `json:"inputs"`
	Totals        Totals       `json:"totals"`
	Files         []FileReport `json:"files"`
	Errors        []string     `json:"errors,omitempty"`
}

type Totals struct {
	InspectedFiles int            `json:"inspectedFiles"`
	TSSPFiles      int            `json:"tsspFiles"`
	MergeSetParts  int            `json:"mergeSetParts"`
	BloomFiles     int            `json:"bloomFiles"`
	SkippedFiles   int            `json:"skippedFiles"`
	Findings       map[string]int `json:"findings"`
}

type Finding struct {
	Severity string `json:"severity,omitempty"`
	File     string `json:"file,omitempty"`
	Message  string `json:"message"`
}

type FileReport struct {
	Path     string         `json:"path"`
	Type     string         `json:"type"`
	Size     int64          `json:"size,omitempty"`
	Status   string         `json:"status"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Findings []Finding      `json:"findings,omitempty"`
}

type analyzer struct {
	opts     Options
	report   Report
	seen     map[string]struct{}
	maxFiles int
}

func Analyze(opts Options) (Report, error) {
	maxFiles := opts.MaxFiles
	if maxFiles <= 0 {
		maxFiles = defaultMaxFiles
	}
	a := &analyzer{
		opts: opts,
		report: Report{
			SchemaVersion: 1,
			Tool:          ToolName,
			Inputs:        append([]string{}, opts.Inputs...),
			Totals: Totals{
				Findings: make(map[string]int),
			},
		},
		seen:     make(map[string]struct{}),
		maxFiles: maxFiles,
	}

	if len(opts.Inputs) == 0 {
		return a.report, fmt.Errorf("at least one input is required")
	}

	for _, input := range opts.Inputs {
		trimmed := strings.TrimSpace(input)
		if trimmed == "" {
			continue
		}
		if err := a.inspectInput(filepath.Clean(trimmed)); err != nil && !errors.Is(err, errMaxFiles) {
			a.report.Errors = append(a.report.Errors, fmt.Sprintf("%s: %v", trimmed, err))
			a.addFinding("high", trimmed, fmt.Sprintf("input could not be inspected: %v", err))
		}
	}

	sort.Slice(a.report.Files, func(i, j int) bool {
		return a.report.Files[i].Path < a.report.Files[j].Path
	})
	a.report.Summary = a.summary()
	return a.report, nil
}

func (a *analyzer) inspectInput(input string) error {
	info, err := os.Lstat(input)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		a.report.Totals.SkippedFiles++
		a.addFinding("medium", input, "skipped symbolic link input")
		return nil
	}
	if info.IsDir() {
		return a.walkDir(input)
	}
	return a.inspectFile(input, info, true)
}

func (a *analyzer) walkDir(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			a.report.Errors = append(a.report.Errors, fmt.Sprintf("%s: %v", path, err))
			a.addFinding("medium", path, fmt.Sprintf("walk error: %v", err))
			return nil
		}
		if path == root {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			a.report.Totals.SkippedFiles++
			a.addFinding("medium", path, "skipped symbolic link")
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			if isMergeSetPartName(filepath.Base(path)) {
				if err := a.inspectMergeSetPart(path); err != nil {
					a.report.Errors = append(a.report.Errors, fmt.Sprintf("%s: %v", path, err))
				}
				return filepath.SkipDir
			}
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil {
			a.report.Errors = append(a.report.Errors, fmt.Sprintf("%s: %v", path, statErr))
			a.addFinding("medium", path, fmt.Sprintf("stat error: %v", statErr))
			return nil
		}
		if err := a.inspectFile(path, info, false); err != nil {
			if errors.Is(err, errMaxFiles) {
				return err
			}
			a.report.Errors = append(a.report.Errors, fmt.Sprintf("%s: %v", path, err))
		}
		return nil
	})
}

func (a *analyzer) inspectFile(path string, info os.FileInfo, direct bool) error {
	if a.report.Totals.InspectedFiles >= a.maxFiles {
		a.addFinding("medium", path, fmt.Sprintf("stopped after max files limit %d", a.maxFiles))
		return errMaxFiles
	}

	base := filepath.Base(path)
	parent := filepath.Dir(path)
	if isMergeSetPartName(filepath.Base(parent)) && isMergeSetPartFile(base) {
		return a.inspectMergeSetPart(parent)
	}
	if isTSSPName(base) || looksLikeTSSP(path) {
		return a.inspectTSSP(path, info)
	}
	if isBloomFilterFile(base) {
		return a.inspectBloomFile(path, info)
	}
	if direct {
		a.report.Totals.SkippedFiles++
		a.addFileReport(FileReport{
			Path:   path,
			Type:   "unknown",
			Size:   info.Size(),
			Status: "SKIPPED",
			Findings: []Finding{{
				Severity: "low",
				File:     path,
				Message:  "input is not a recognized TSSP or mergeset file",
			}},
		})
	}
	return nil
}

func (a *analyzer) inspectTSSP(path string, info os.FileInfo) error {
	if a.seenPath(path) {
		return nil
	}
	a.report.Totals.InspectedFiles++
	a.report.Totals.TSSPFiles++

	fr := FileReport{
		Path:     path,
		Type:     "tssp",
		Size:     info.Size(),
		Status:   "OK",
		Metadata: make(map[string]any),
	}

	if name, ok := parseTSSPName(filepath.Base(path)); ok {
		fr.Metadata["fileName"] = name
		if name.Temporary {
			fr.Findings = append(fr.Findings, finding("medium", path, "TSSP file still has .init temporary suffix"))
		}
	} else {
		fr.Findings = append(fr.Findings, finding("medium", path, "TSSP filename does not match openGemini naming convention"))
	}

	if info.Size() < int64(tsspHeaderSize+tsspFooterSize) {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "TSSP file is too small to contain header and footer"))
		a.addFileReport(fr)
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, fmt.Sprintf("failed to open TSSP file: %v", err)))
		a.addFileReport(fr)
		return nil
	}
	defer file.Close()

	header := make([]byte, tsspHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, fmt.Sprintf("failed to read TSSP header: %v", err)))
		a.addFileReport(fr)
		return nil
	}
	magic := string(header[:len(tsspMagic)])
	version := binary.BigEndian.Uint64(header[len(tsspMagic):])
	fr.Metadata["magic"] = magic
	fr.Metadata["version"] = version
	if magic != tsspMagic {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, fmt.Sprintf("invalid TSSP magic %q", magic)))
	}

	footer := make([]byte, tsspFooterSize)
	if _, err := file.ReadAt(footer, info.Size()-tsspFooterSize); err != nil {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, fmt.Sprintf("failed to read TSSP footer: %v", err)))
		a.addFileReport(fr)
		return nil
	}
	trailerOffset := decodeInt64(footer)
	trailerSize := info.Size() - tsspFooterSize - trailerOffset
	fr.Metadata["trailerOffset"] = trailerOffset
	fr.Metadata["trailerSize"] = trailerSize
	if trailerOffset < int64(tsspHeaderSize) || trailerOffset > info.Size()-tsspFooterSize || trailerSize <= 0 {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "invalid TSSP trailer offset from footer"))
		a.addFileReport(fr)
		return nil
	}
	if trailerSize > maxTrailerReadBytes {
		fr.Findings = append(fr.Findings, finding("medium", path, fmt.Sprintf("TSSP trailer is too large to parse safely: %d bytes", trailerSize)))
		a.addFileReport(fr)
		return nil
	}

	trailer := make([]byte, trailerSize)
	if _, err := file.ReadAt(trailer, trailerOffset); err != nil {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, fmt.Sprintf("failed to read TSSP trailer: %v", err)))
		a.addFileReport(fr)
		return nil
	}
	stat, err := parseTSSPTrailer(trailer)
	if err != nil {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, fmt.Sprintf("failed to parse TSSP trailer: %v", err)))
		a.addFileReport(fr)
		return nil
	}
	fr.Metadata["trailer"] = stat
	if stat.SectionEnd != trailerOffset {
		severity := "medium"
		if stat.SectionEnd > trailerOffset {
			severity = "high"
			fr.Status = "ERROR"
		}
		fr.Findings = append(fr.Findings, finding(severity, path, fmt.Sprintf("TSSP section end %d does not match trailer offset %d", stat.SectionEnd, trailerOffset)))
	}
	if stat.IDCount < 0 {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "TSSP trailer has negative idCount"))
	}
	if stat.MinID > stat.MaxID {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "TSSP trailer minId is greater than maxId"))
	}
	if stat.MinTime > stat.MaxTime {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "TSSP trailer minTime is greater than maxTime"))
	}
	if stat.MetaIndexItemNum < 0 {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "TSSP trailer has negative metaIndexItemNum"))
	}

	a.addFileReport(fr)
	return nil
}

func (a *analyzer) inspectMergeSetPart(path string) error {
	if a.seenPath(path) {
		return nil
	}
	a.report.Totals.InspectedFiles++
	a.report.Totals.MergeSetParts++

	fr := FileReport{
		Path:     path,
		Type:     "mergeset_part",
		Status:   "OK",
		Metadata: make(map[string]any),
	}
	part, ok := parseMergeSetPartName(filepath.Base(path))
	if !ok {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "invalid mergeset part directory name"))
		a.addFileReport(fr)
		return nil
	}
	fr.Metadata["partName"] = part
	if part.ItemsCount == 0 {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "mergeset part item count is zero"))
	}
	if part.BlocksCount == 0 {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "mergeset part block count is zero"))
	}
	if part.BlocksCount > part.ItemsCount {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", path, "mergeset part block count exceeds item count"))
	}

	fileSizes := make(map[string]int64)
	for _, name := range []string{"metadata.json", "metaindex.bin", "index.bin", "items.bin", "lens.bin"} {
		child := filepath.Join(path, name)
		info, err := os.Lstat(child)
		if err != nil {
			fr.Status = "ERROR"
			fr.Findings = append(fr.Findings, finding("high", child, fmt.Sprintf("required mergeset part file is missing: %s", name)))
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			fr.Status = "ERROR"
			fr.Findings = append(fr.Findings, finding("high", child, "required mergeset part file is a symbolic link"))
			continue
		}
		if info.IsDir() {
			fr.Status = "ERROR"
			fr.Findings = append(fr.Findings, finding("high", child, "required mergeset part file is a directory"))
			continue
		}
		fileSizes[name] = info.Size()
		fr.Size += info.Size()
		if info.Size() == 0 {
			fr.Findings = append(fr.Findings, finding("medium", child, "required mergeset part file is empty"))
		}
	}
	fr.Metadata["fileSizes"] = fileSizes

	metadataPath := filepath.Join(path, "metadata.json")
	if metadata, err := readPartMetadata(metadataPath); err != nil {
		fr.Status = "ERROR"
		fr.Findings = append(fr.Findings, finding("high", metadataPath, fmt.Sprintf("failed to parse mergeset metadata.json: %v", err)))
	} else {
		fr.Metadata["metadata"] = metadata
		if metadata.ItemsCount != part.ItemsCount {
			fr.Status = "ERROR"
			fr.Findings = append(fr.Findings, finding("high", metadataPath, fmt.Sprintf("metadata ItemsCount %d does not match directory item count %d", metadata.ItemsCount, part.ItemsCount)))
		}
		if metadata.BlocksCount != part.BlocksCount {
			fr.Status = "ERROR"
			fr.Findings = append(fr.Findings, finding("high", metadataPath, fmt.Sprintf("metadata BlocksCount %d does not match directory block count %d", metadata.BlocksCount, part.BlocksCount)))
		}
		if metadata.FirstItem == "" || metadata.LastItem == "" {
			fr.Findings = append(fr.Findings, finding("low", metadataPath, "mergeset metadata has empty first or last item"))
		}
	}

	a.addFileReport(fr)
	return nil
}

func (a *analyzer) inspectBloomFile(path string, info os.FileInfo) error {
	if a.seenPath(path) {
		return nil
	}
	a.report.Totals.InspectedFiles++
	a.report.Totals.BloomFiles++
	fr := FileReport{
		Path:     path,
		Type:     "mergeset_bloom_filter",
		Size:     info.Size(),
		Status:   "OK",
		Metadata: map[string]any{"fileName": filepath.Base(path)},
	}
	if strings.HasSuffix(path, ".init") {
		fr.Findings = append(fr.Findings, finding("medium", path, "bloom filter file has temporary .init suffix"))
	}
	if strings.HasSuffix(path, ".last") {
		fr.Findings = append(fr.Findings, finding("low", path, "bloom filter file has .last rollover suffix"))
	}
	if info.Size() == 0 {
		fr.Findings = append(fr.Findings, finding("medium", path, "bloom filter file is empty"))
	}
	a.addFileReport(fr)
	return nil
}

func (a *analyzer) addFileReport(fr FileReport) {
	for _, item := range fr.Findings {
		a.addFinding(item.Severity, item.File, item.Message)
	}
	a.report.Files = append(a.report.Files, fr)
}

func (a *analyzer) addFinding(severity, file, message string) {
	a.report.Findings = append(a.report.Findings, finding(severity, file, message))
	if severity == "" {
		severity = "unknown"
	}
	a.report.Totals.Findings[severity]++
}

func (a *analyzer) seenPath(path string) bool {
	cleaned := filepath.Clean(path)
	if _, ok := a.seen[cleaned]; ok {
		return true
	}
	a.seen[cleaned] = struct{}{}
	return false
}

func (a *analyzer) summary() string {
	totalFindings := len(a.report.Findings)
	high := a.report.Totals.Findings["high"]
	medium := a.report.Totals.Findings["medium"]
	return fmt.Sprintf(
		"opengemini storage analyzer inspected %d file/part item(s): tssp=%d, mergesetParts=%d, bloomFiles=%d, findings=%d (high=%d, medium=%d)",
		a.report.Totals.InspectedFiles,
		a.report.Totals.TSSPFiles,
		a.report.Totals.MergeSetParts,
		a.report.Totals.BloomFiles,
		totalFindings,
		high,
		medium,
	)
}

func finding(severity, file, message string) Finding {
	return Finding{Severity: severity, File: file, Message: message}
}

type TSSPName struct {
	Sequence  uint64 `json:"sequence"`
	Level     uint16 `json:"level"`
	Merge     uint16 `json:"merge"`
	Extent    uint16 `json:"extent"`
	Temporary bool   `json:"temporary"`
}

func parseTSSPName(name string) (TSSPName, bool) {
	match := tsspNameRE.FindStringSubmatch(name)
	if match == nil {
		return TSSPName{}, false
	}
	seq, err := strconv.ParseUint(match[1], 16, 64)
	if err != nil {
		return TSSPName{}, false
	}
	level, err := strconv.ParseUint(match[2], 16, 16)
	if err != nil {
		return TSSPName{}, false
	}
	merge, err := strconv.ParseUint(match[3], 16, 16)
	if err != nil {
		return TSSPName{}, false
	}
	extent, err := strconv.ParseUint(match[4], 16, 16)
	if err != nil {
		return TSSPName{}, false
	}
	return TSSPName{
		Sequence:  seq,
		Level:     uint16(level),
		Merge:     uint16(merge),
		Extent:    uint16(extent),
		Temporary: match[5] == ".init",
	}, true
}

func isTSSPName(name string) bool {
	_, ok := parseTSSPName(name)
	return ok
}

func looksLikeTSSP(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	header := make([]byte, len(tsspMagic))
	if _, err := io.ReadFull(file, header); err != nil {
		return false
	}
	return string(header) == tsspMagic
}

type TSSPTrailer struct {
	DataOffset            int64  `json:"dataOffset"`
	DataSize              int64  `json:"dataSize"`
	IndexSize             int64  `json:"indexSize"`
	MetaIndexSize         int64  `json:"metaIndexSize"`
	BloomSize             int64  `json:"bloomSize"`
	IDTimeSize            int64  `json:"idTimeSize"`
	IDCount               int64  `json:"idCount"`
	MinID                 uint64 `json:"minId"`
	MaxID                 uint64 `json:"maxId"`
	MinTime               int64  `json:"minTime"`
	MaxTime               int64  `json:"maxTime"`
	MetaIndexItemNum      int64  `json:"metaIndexItemNum"`
	BloomM                uint64 `json:"bloomM"`
	BloomK                uint64 `json:"bloomK"`
	TimeStoreFlag         uint8  `json:"timeStoreFlag"`
	ChunkMetaCompressFlag uint8  `json:"chunkMetaCompressFlag"`
	ChunkMetaHeaderValues int    `json:"chunkMetaHeaderValues"`
	Measurement           string `json:"measurement"`
	SectionEnd            int64  `json:"sectionEnd"`
}

func parseTSSPTrailer(buf []byte) (TSSPTrailer, error) {
	var t TSSPTrailer
	pos := 0
	readInt64 := func(label string) (int64, error) {
		if len(buf[pos:]) < 8 {
			return 0, fmt.Errorf("trailer missing %s", label)
		}
		value := decodeInt64(buf[pos : pos+8])
		pos += 8
		return value, nil
	}
	readUint64 := func(label string) (uint64, error) {
		if len(buf[pos:]) < 8 {
			return 0, fmt.Errorf("trailer missing %s", label)
		}
		value := binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		return value, nil
	}

	var err error
	if t.DataOffset, err = readInt64("dataOffset"); err != nil {
		return t, err
	}
	if t.DataSize, err = readInt64("dataSize"); err != nil {
		return t, err
	}
	if t.IndexSize, err = readInt64("indexSize"); err != nil {
		return t, err
	}
	if t.MetaIndexSize, err = readInt64("metaIndexSize"); err != nil {
		return t, err
	}
	if t.BloomSize, err = readInt64("bloomSize"); err != nil {
		return t, err
	}
	if t.IDTimeSize, err = readInt64("idTimeSize"); err != nil {
		return t, err
	}
	if t.IDCount, err = readInt64("idCount"); err != nil {
		return t, err
	}
	if t.MinID, err = readUint64("minId"); err != nil {
		return t, err
	}
	if t.MaxID, err = readUint64("maxId"); err != nil {
		return t, err
	}
	if t.MinTime, err = readInt64("minTime"); err != nil {
		return t, err
	}
	if t.MaxTime, err = readInt64("maxTime"); err != nil {
		return t, err
	}
	if t.MetaIndexItemNum, err = readInt64("metaIndexItemNum"); err != nil {
		return t, err
	}
	if t.BloomM, err = readUint64("bloomM"); err != nil {
		return t, err
	}
	if t.BloomK, err = readUint64("bloomK"); err != nil {
		return t, err
	}

	if len(buf[pos:]) < 2 {
		return t, fmt.Errorf("trailer missing extra data length")
	}
	extraLen := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2
	if len(buf[pos:]) < extraLen {
		return t, fmt.Errorf("extra data too short")
	}
	actualExtraLen := extraLen
	if extraLen == 1 {
		t.TimeStoreFlag = buf[pos]
	} else if extraLen == 2 {
		t.TimeStoreFlag = buf[pos]
		t.ChunkMetaCompressFlag = buf[pos+1]
	} else if extraLen >= 8 {
		flags := binary.LittleEndian.Uint64(buf[pos : pos+8])
		t.TimeStoreFlag = uint8(flags & 0xff)
		t.ChunkMetaCompressFlag = uint8((flags >> 8) & 0xff)
		if size := int(flags >> 32); size > 0 {
			actualExtraLen = size
		}
		if len(buf[pos:]) < actualExtraLen {
			return t, fmt.Errorf("extra data actual length %d exceeds remaining trailer", actualExtraLen)
		}
		if actualExtraLen >= 10 {
			t.ChunkMetaHeaderValues = int(binary.BigEndian.Uint16(buf[pos+8 : pos+10]))
		}
	}
	pos += actualExtraLen

	if len(buf[pos:]) < 2 {
		return t, fmt.Errorf("trailer missing measurement name length")
	}
	nameLen := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2
	if len(buf[pos:]) < nameLen {
		return t, fmt.Errorf("measurement name length %d exceeds remaining trailer", nameLen)
	}
	t.Measurement = string(buf[pos : pos+nameLen])
	t.SectionEnd = t.DataOffset + t.DataSize + t.IndexSize + t.MetaIndexSize + t.BloomSize + t.IDTimeSize
	return t, nil
}

func decodeInt64(src []byte) int64 {
	u := binary.BigEndian.Uint64(src)
	return int64(u>>1) ^ (int64(u<<63) >> 63)
}

type MergeSetPartName struct {
	ItemsCount  uint64 `json:"itemsCount"`
	BlocksCount uint64 `json:"blocksCount"`
	Suffix      string `json:"suffix"`
}

type MergeSetMetadata struct {
	ItemsCount  uint64 `json:"ItemsCount"`
	BlocksCount uint64 `json:"BlocksCount"`
	FirstItem   string `json:"FirstItem"`
	LastItem    string `json:"LastItem"`
}

func parseMergeSetPartName(name string) (MergeSetPartName, bool) {
	match := mergesetPartRE.FindStringSubmatch(name)
	if match == nil {
		return MergeSetPartName{}, false
	}
	items, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return MergeSetPartName{}, false
	}
	blocks, err := strconv.ParseUint(match[2], 10, 64)
	if err != nil {
		return MergeSetPartName{}, false
	}
	return MergeSetPartName{ItemsCount: items, BlocksCount: blocks, Suffix: match[3]}, true
}

func isMergeSetPartName(name string) bool {
	_, ok := parseMergeSetPartName(name)
	return ok
}

func isMergeSetPartFile(name string) bool {
	switch name {
	case "metadata.json", "metaindex.bin", "index.bin", "items.bin", "lens.bin":
		return true
	default:
		return false
	}
}

func readPartMetadata(path string) (MergeSetMetadata, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return MergeSetMetadata{}, err
	}
	var metadata MergeSetMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return MergeSetMetadata{}, err
	}
	return metadata, nil
}

func isBloomFilterFile(name string) bool {
	return bloomFilterFileRE.MatchString(name)
}
