# QVF Extraction Findings

## Purpose

This document captures the current reverse-engineering findings for the direct QVF extractor in this repository.
It is intended to be technical enough to support further parser work, especially around table reconstruction.

The notes below are based on the current extractor implementation and the sample file:

- `Asset Management.qvf`

## Repeatable Analysis Command

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/extract_qvf.py "Asset Management.qvf" \
  --output-dir artifacts/output/asset-management \
  --zip artifacts/output/asset-management.zip
```

## Output Contract

The extractor currently writes:

- `manifest.json`
- `app.json`
- `sheets.json`
- `masterobjects.json`
- `measures.json`
- `dimensions.json`
- `variables.json`
- `data-model.json`
- `data-sources.json`
- `load-model.json`
- `color-maps.json`
- `script.qvs`
- `assets.json`
- `tables/_manifest.json`
- `tables/_confidence.json`
- `tables/<table>.parquet`
- `tables/<table>.tsv`
- `raw/blocks.jsonl`
- `raw/decoded-objects.jsonl`
- `raw/non-scalar-streams.json`
- `raw/scalar-streams.json`
- `raw/table-block-mapping.json`
- `raw/unknown-blocks.json`
- `raw/string-findings.txt`
- `summary.txt`

## File Structure Findings

### Format records

The file contains top-level records marked by:

```text
{"format":"gzjson"}\x00
{"format":"binary"}\x00
```

The current parser treats the record layout as:

1. format marker
2. `declared_size` as little-endian `uint32`
3. `stored_size` as little-endian `uint32`
4. stored payload bytes

For `gzjson` payloads, `zlib.decompress(payload)` yields JSON text.
For `binary` payloads, the same payload may contain images or other binary blocks.

### Header window metadata

A printable window before the format marker contains useful side-channel metadata such as:

- `ContentHash`
- `Format`
- `Type`
- `SharedStatus`
- `SecurityMetaAsBase64`
- candidate asset file names

This is parsed by `parse_header_metadata()` and used for classification and asset naming.

## Structured JSON Coverage

The current `gzjson` decoder is reliable for the following object families:

- app root (`qTitle`, product version, reload metadata)
- app properties
- sheets
- master objects
- measures
- dimensions
- variable lists
- load model
- data model metadata
- color maps
- load script container (`qScript`)

For `Asset Management.qvf`, this is enough to export:

- app metadata
- 5 sheets
- measures
- dimensions
- variables
- the full load script as `script.qvs`
- the data model definition from `data-model.json`

## Load Script Handling

The script is extracted from the JSON object containing `qScript` and stored exactly as:

- `script.qvs`

This is already suitable for downstream tooling.

The script also provides exact reconstruction hints for inline tables.

### Confirmed inline-table case

`Colors` is defined inline in the script and can therefore be reconstructed exactly from script text alone.

Current rule:

- parse `Load * Inline [ ... ];`
- split the header row and subsequent rows
- use the script version as the authoritative source when present

## Scalar Stream Findings

Beyond the top-level `gzjson` and `binary` records, the file contains many additional zlib streams.
A subset of these follows a simple scalar encoding that is now decoded by `parse_scalar_stream()`.

Current layout assumption:

1. 4-byte prefix or reserved field
2. 4-byte little-endian value count
3. repeated typed values

Typed values currently confirmed:

- `0x04 <len:1> <utf8 bytes>` for strings
- `0x02 <8-byte little-endian double>` for floating-point values
- `0x05 <len:1> <little-endian integer bytes>` for integers

The parser only accepts streams where every decoded value has the same type.

### Confirmed exact scalar dictionaries in `Asset Management.qvf`

- Offset `1115706`: `_CURRENCY` values `USD`, `EUR`, `GBP`
- Offset `136762`: `_SYMBOL` values `$`, `€`, `£`
- Offset `103482`: `Asset Class` values `Equity`, `Fixed Income`, `Alternatives`, `Multi-Asset`
- Offset `1117754`: color values `#008945`, `#19426C`, `#10CFC9`, `#999A9C`
- Offset `527433`: 10 holdings names
- Offset `1112649`: 10 sector names
- Offset `528449`: 9 numeric values aligned to `Holdings.YTD` with 1 unresolved row
- Offset `1113665`: 9 numeric values aligned to `Composition.Sector YTD` with 1 unresolved row
- Offset `1114689`: 9 numeric values aligned to `Composition.Sector LYTD` with 1 unresolved row

### Confirmed row-wise scalar streams in `Asset Management.qvf`

- Offset `95295`: 540 numeric identifier-like values, currently mapped heuristically to `Performance.PerformanceId`
- Offset `556090`: 15361 numeric identifier-like values, currently mapped heuristically to `Trades.Trade ID`

These are row-count-matching streams and are currently the strongest row-aligned evidence for the larger fact tables.

## Table Reconstruction Rules

### Canonical schema source

`data-model.json` is the canonical schema source.

The extractor uses:

- `qtables[].qname`
- `qtables[].qno_of_rows`
- `qfields[].qsrc_tables`
- `qfields[].qcardinal`
- `qfields[].qtotal_count`
- `qfields[].qis_numeric`
- `qfields[].qtags`

### Current table export statuses for `Asset Management.qvf`

- `Currency`: exact
- `Colors`: exact
- `Holdings`: partial
- `Composition`: partial
- `Performance`: partial
- `Trades`: partial
- `AUM`: missing
- `Positions`: missing
- `Risk`: missing
- `$$SysTable 9`: missing
- `$$SysTable 10`: missing
- `$$SysTable 11`: missing

### Exact reconstruction cases

1. `Currency`
   - `_CURRENCY` from scalar dictionary at `1115706`
   - `_SYMBOL` from scalar dictionary at `136762`

2. `Colors`
   - reconstructed from inline script table
   - scalar dictionaries exist as secondary evidence but script is preferred

### Partial reconstruction cases

1. `Holdings`
   - `Holdings` exact from scalar dictionary at `527433`
   - `YTD` partial from 9-value numeric dictionary at `528449`
   - current behavior pads unresolved rows with null

2. `Composition`
   - `Sector` exact from scalar dictionary at `1112649`
   - `Sector YTD` partial from `1113665`
   - `Sector LYTD` partial from `1114689`
   - current behavior pads unresolved rows with null

3. `Performance`
   - `PerformanceId` heuristic row-wise assignment from `95295`
   - other columns remain unresolved

4. `Trades`
   - `Trade ID` heuristic row-wise assignment from `556090`
   - other columns remain unresolved

### Matching heuristics currently in code

The row-wise matcher uses deterministic scoring based on:

- exact row-count match
- value type compatibility
- unique-count vs field cardinality
- numeric integral behavior
- identifier naming (`...Id`)
- date-like numeric ranges
- selected field-name patterns such as `currency`, `symbol`, `link`, `status`
- penalties for clearly dictionary-like key columns where row-wise mapping would be misleading

The matcher now assigns streams globally by best score, rather than greedily by field order.
This avoids one strong row stream being claimed by a weaker earlier field.

## Non-Scalar Zlib Blocks

There are many valid zlib streams that are not yet decoded by the scalar parser.
These are important for future full-table reconstruction.

Observed patterns include:

- long zero-padded blocks
- compact byte-packed or bit-packed sequences
- repeated 16-bit / 32-bit packed values
- blocks whose decompressed size tracks expected row-vector or dictionary-index sizes
- blocks that appear in clustered series around related table areas

Representative offsets from `Asset Management.qvf`:

- `1124324`
- `1124933`
- `1125842`
- `1147005`
- `1211590`
- `1234679`
- `1249838`
- `1264997`
- `1288871`
- `1359366`
- `1412793`
- `1483670`
- `1712285`
- `1803020`
- `1852663`
- `1936754`
- `1998959`
- `2054730`

These are the main candidates for the missing row vectors, dictionary-index arrays, or compressed column stores needed for `AUM`, `Positions`, and `Risk`.

## What Can Be Trusted Today

### High confidence

- top-level `gzjson` extraction
- load script extraction to `script.qvs`
- app, sheet, measure, dimension, variable, and data model exports
- exact lookup-table export for `Currency` and `Colors`
- exact lookup-column export for `Holdings.Holdings` and `Composition.Sector`
- partial numeric dictionary export for `Holdings` and `Composition`
- heuristic identifier export for `PerformanceId` and `Trade ID`

### Medium confidence

- asset extraction from bounded binary payloads
- row-wise assignment for `PerformanceId` and `Trade ID`
- candidate dictionary offsets recorded in `tables/_confidence.json`

### Low confidence / unresolved

- full reconstruction of `AUM`
- full reconstruction of `Positions`
- full reconstruction of `Risk`
- row alignment for partial 10-row tables where only 9 numeric values were recovered
- decoding of the clustered non-scalar zlib blocks listed above

## How To Continue The Extractor

### Priority 1

Build a dedicated decoder for non-scalar zlib streams and classify them into:

- dictionary-index vectors
- row-position vectors
- bit-packed numeric columns
- compressed numeric pages
- unresolved binary blobs

### Priority 2

Correlate non-scalar streams to fields using:

- `qcardinal`
- `qtotal_count`
- expected row count per table
- value repetition patterns
- nearby dictionary offsets
- repeated block clusters with identical decompressed lengths

### Priority 3

Add table-level vector assembly:

- dictionary values
- index vectors
- row vectors
- null masks
- final row-major export

## Current Repository Files That Matter Most

- `scripts/extract_qvf.py`
- `requirements.txt`
- `server/index.js`
- `public/index.html`
- `README.md`

## Practical Conclusion

The repository is now in a good place for metadata extraction and for confidence-marked table export.
The remaining work is no longer about whether the QVF can be read directly, but about decoding the still-opaque non-scalar zlib blocks that appear to hold the heavier table storage structures.
