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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/openGemini/openGemini/lib/storageanalyzer"
)

type inputFlags []string

func (f *inputFlags) String() string {
	return fmt.Sprintf("%v", []string(*f))
}

func (f *inputFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func main() {
	var inputs inputFlags
	format := flag.String("format", "json", "output format; only json is supported")
	pretty := flag.Bool("pretty", false, "pretty-print JSON output")
	maxFiles := flag.Int("max-files", 512, "maximum recognized files or part directories to inspect")
	flag.Var(&inputs, "input", "input TSSP file, mergeset file, mergeset part directory, or parent directory; may be repeated")
	flag.Parse()

	inputs = append(inputs, flag.Args()...)
	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr, "at least one --input or positional input is required")
		flag.Usage()
		os.Exit(2)
	}
	if *format != "json" {
		fmt.Fprintf(os.Stderr, "unsupported format %q\n", *format)
		os.Exit(2)
	}

	report, err := storageanalyzer.Analyze(storageanalyzer.Options{
		Inputs:   inputs,
		MaxFiles: *maxFiles,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	encoder := json.NewEncoder(os.Stdout)
	if *pretty {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "failed to encode report: %v\n", err)
		os.Exit(1)
	}
}
