# openGemini Storage Analyzer

`opengemini-storage-analyzer` is a read-only CLI for inspecting openGemini storage artifacts. The first MVP focuses on safe validation and JSON evidence output for:

- TSSP files: filename fields, magic/version, footer trailer offset, core trailer metadata, section boundary checks, time/id range anomalies, and temporary `.init` files.
- TSI/mergeset parts: part directory name, `metadata.json`, required part files, count consistency, empty files, and bloom filter rollover/temp files.

The tool does not repair files, rewrite metadata, follow symlinks, or require a running openGemini process.

## CLI

```bash
go build -o build/opengemini-storage-analyzer ./app/opengemini-storage-analyzer

./build/opengemini-storage-analyzer \
  --input /path/to/00000001-0000-00000000.tssp \
  --format json
```

`--input` may be repeated and may point to a file, a mergeset part directory, or a parent directory. Directories are scanned recursively without following symlinks. Use `--max-files` to cap the number of recognized files or part directories inspected.

The JSON stdout contains top-level `summary` and `findings` fields so external runners such as LogAgent Tool Runner can consume it directly.
