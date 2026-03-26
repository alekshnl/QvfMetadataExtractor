#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import re
import struct
import sys
import uuid
import zlib
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised in runtime checks
    pa = None
    pq = None

FORMAT_MARKER_RE = re.compile(rb'\{"format":"(gzjson|binary)"\}\x00')
ASCII_STRING_RE = re.compile(rb'[\x20-\x7e]{6,}')
UTF16LE_STRING_RE = re.compile(rb'(?:[\x20-\x7e]\x00){6,}')
WINDOW_BYTES = 1400
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".bmp")
INTEGER_TAG = 5
STRING_TAG = 4
DOUBLE_TAG = 2
QLIK_EPOCH = datetime(1899, 12, 30)
STANDARD_CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract metadata and best-effort table data from a QVF file without a Qlik runtime."
    )
    parser.add_argument("input_file", help="Path to the .qvf file to analyze")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where extracted metadata files are written",
    )
    parser.add_argument(
        "--zip",
        dest="zip_path",
        help="Optional path to write a ZIP archive of the output directory",
    )
    parser.add_argument(
        "--raw-dir-name",
        default="raw",
        help="Subdirectory name for raw analysis artifacts",
    )
    parser.add_argument(
        "--skip-tables",
        action="store_true",
        help="Skip table reconstruction and omit the tables/ output directory.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_dump(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def jsonl_dump(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def printable_window(chunk: bytes) -> str:
    return "".join(chr(byte) for byte in chunk if byte in (9, 10, 13) or 32 <= byte < 127)


def parse_header_metadata(window: bytes) -> dict[str, Any]:
    text = printable_window(window)
    metadata: dict[str, Any] = {"window_text": text[-1000:]}

    patterns = {
        "content_hash": r'"ContentHash":"([^"]+)"',
        "format": r'"Format":"([^"]+)"',
        "type": r'"Type":"([^"]+)"',
        "shared_status": r'"SharedStatus":"([^"]+)"',
        "security_meta_base64": r'SecurityMetaAsBase64":"([A-Za-z0-9+/=]+)"',
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            metadata[key] = match.group(1)

    candidate_names: list[str] = []
    for match in re.findall(r"([A-Za-z0-9 _./-]+\.(?:png|jpg|jpeg|gif|bmp))", text, flags=re.IGNORECASE):
        cleaned = match.strip().strip('"').replace("pngk", "png").replace("jpgk", "jpg")
        if any(cleaned.lower().endswith(ext) for ext in IMAGE_EXTENSIONS):
            candidate_names.append(cleaned)
    if candidate_names:
        metadata["filenames"] = list(dict.fromkeys(candidate_names))

    encoded = metadata.get("security_meta_base64")
    if encoded:
        try:
            metadata["security_meta"] = json.loads(base64.b64decode(encoded).decode("utf-8"))
        except Exception:
            pass

    return metadata


def classify_payload(obj: Any) -> str:
    if not isinstance(obj, dict):
        return "unknown"
    if "qTitle" in obj:
        return "app"
    if "qScript" in obj:
        return "script"
    if "qreload_meta" in obj:
        return "data_model_metadata"
    if obj.get("qMetaData", {}).get("qType"):
        return f"meta:{obj['qMetaData']['qType']}"
    if obj.get("qInfo", {}).get("qType"):
        return f"info:{obj['qInfo']['qType']}"
    if obj.get("qId") and "qEntryList" in obj:
        return f"entrylist:{obj['qId']}"
    return "unknown"


def decode_format_blocks(blob: bytes) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocks: list[dict[str, Any]] = []
    decoded_objects: list[dict[str, Any]] = []

    for match in FORMAT_MARKER_RE.finditer(blob):
        format_name = match.group(1).decode("ascii")
        marker_offset = match.start()
        size_offset = match.end()
        if size_offset + 8 > len(blob):
            continue

        declared_size = int.from_bytes(blob[size_offset : size_offset + 4], "little")
        stored_size = int.from_bytes(blob[size_offset + 4 : size_offset + 8], "little")
        payload_offset = size_offset + 8
        payload_end = payload_offset + stored_size
        if payload_end > len(blob):
            continue

        header = parse_header_metadata(blob[max(0, marker_offset - WINDOW_BYTES) : marker_offset])
        block: dict[str, Any] = {
            "block_id": str(uuid.uuid4()),
            "marker_offset": marker_offset,
            "payload_offset": payload_offset,
            "payload_end": payload_end,
            "format": format_name,
            "declared_size": declared_size,
            "stored_size": stored_size,
            "header": header,
        }

        payload = blob[payload_offset:payload_end]
        if format_name == "gzjson":
            try:
                decompressed = zlib.decompress(payload)
            except zlib.error as error:
                block["status"] = "decompression_failed"
                block["error"] = str(error)
                blocks.append(block)
                continue

            block["status"] = "decoded"
            block["decompressed_size"] = len(decompressed)
            block["declared_size_matches"] = len(decompressed) == declared_size
            block["payload_sha256"] = sha256_bytes(decompressed)
            text = decompressed.rstrip(b"\x00").decode("utf-8", errors="replace")
            block["text_preview"] = text[:240]

            try:
                obj = json.loads(text)
                classification = classify_payload(obj)
                block["classification"] = classification
                decoded_objects.append(
                    {
                        "marker_offset": marker_offset,
                        "payload_offset": payload_offset,
                        "classification": classification,
                        "header_type": header.get("type"),
                        "header": {k: v for k, v in header.items() if k != "window_text"},
                        "object": obj,
                    }
                )
            except json.JSONDecodeError as error:
                block["classification"] = "text"
                block["error"] = f"JSON decode failed: {error}"

            blocks.append(block)
            continue

        block["status"] = "binary"
        block["classification"] = header.get("type", "binary")
        block["payload_sha256"] = sha256_bytes(payload)
        blocks.append(block)

    return blocks, decoded_objects


def detect_png_end(payload: bytes, start: int) -> int | None:
    marker = b"IEND\xaeB`\x82"
    end = payload.find(marker, start)
    if end < 0:
        return None
    return end + len(marker)


def detect_jpeg_end(payload: bytes, start: int) -> int | None:
    end = payload.find(b"\xff\xd9", start + 3)
    if end < 0:
        return None
    return end + 2


def detect_gif_end(payload: bytes, start: int) -> int | None:
    end = payload.find(b"\x3b", start + 6)
    if end < 0:
        return None
    return end + 1


def detect_bmp_end(payload: bytes, start: int) -> int | None:
    if start + 6 > len(payload):
        return None
    size = int.from_bytes(payload[start + 2 : start + 6], "little")
    if size <= 0 or start + size > len(payload):
        return None
    return start + size


def extract_binary_assets(
    blob: bytes, blocks: list[dict[str, Any]], assets_dir: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ensure_dir(assets_dir)
    assets: list[dict[str, Any]] = []
    residual_blocks: list[dict[str, Any]] = []

    for block in blocks:
        if block.get("format") != "binary":
            continue

        payload = blob[block["payload_offset"] : block["payload_end"]]
        try:
            decompressed = zlib.decompress(payload)
        except zlib.error as error:
            residual_blocks.append(
                {
                    "marker_offset": block["marker_offset"],
                    "format": "binary",
                    "classification": block.get("classification"),
                    "status": "decompression_failed",
                    "error": str(error),
                }
            )
            continue

        block["decompressed_size"] = len(decompressed)
        found_in_block = 0
        filename_candidates = block.get("header", {}).get("filenames", [])
        candidate_index = 0
        scan_patterns = [
            (b"\x89PNG\r\n\x1a\n", "png", detect_png_end),
            (b"\xff\xd8\xff", "jpeg", detect_jpeg_end),
            (b"GIF87a", "gif", detect_gif_end),
            (b"GIF89a", "gif", detect_gif_end),
            (b"BM", "bmp", detect_bmp_end),
        ]

        for signature, asset_type, end_detector in scan_patterns:
            position = 0
            while True:
                position = decompressed.find(signature, position)
                if position < 0:
                    break
                end = end_detector(decompressed, position)
                if end is None or end <= position:
                    position += 1
                    continue

                if candidate_index < len(filename_candidates):
                    candidate_name = filename_candidates[candidate_index]
                    candidate_index += 1
                else:
                    candidate_name = f"asset-{block['marker_offset']}-{found_in_block + 1}.{asset_type}"

                candidate_name = re.sub(r"^[0-9]+(?=[A-Za-z])", "", candidate_name)
                safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", candidate_name).strip("-")
                if not safe_name:
                    safe_name = f"asset-{found_in_block + 1}.{asset_type}"
                if not safe_name.lower().endswith(f".{asset_type}"):
                    safe_name = f"{safe_name}.{asset_type}"

                asset_bytes = decompressed[position:end]
                (assets_dir / safe_name).write_bytes(asset_bytes)
                assets.append(
                    {
                        "filename": safe_name,
                        "type": asset_type,
                        "marker_offset": block["marker_offset"],
                        "payload_offset": block["payload_offset"],
                        "embedded_offset": position,
                        "size": len(asset_bytes),
                        "sha256": sha256_bytes(asset_bytes),
                        "source_type": block.get("classification"),
                        "source_header_filenames": filename_candidates,
                    }
                )
                found_in_block += 1
                position = end

        if found_in_block == 0:
            residual_blocks.append(
                {
                    "marker_offset": block["marker_offset"],
                    "format": "binary",
                    "classification": block.get("classification"),
                    "status": "opaque_binary",
                    "decompressed_size": len(decompressed),
                    "header": {k: v for k, v in block.get("header", {}).items() if k != "window_text"},
                }
            )

    return assets, residual_blocks


def extract_strings(blob: bytes) -> list[str]:
    ascii_strings = [match.decode("utf-8", errors="ignore") for match in ASCII_STRING_RE.findall(blob)]
    utf16_strings = [match.decode("utf-16le", errors="ignore") for match in UTF16LE_STRING_RE.findall(blob)]
    strings = ascii_strings + utf16_strings
    return list(dict.fromkeys(item.strip() for item in strings if item.strip()))


def collect_data_sources(script: str, strings: list[str]) -> dict[str, list[str]]:
    lib_refs = sorted(set(re.findall(r"lib://[^\]\r\n']+", script)))
    raw_file_refs = set(re.findall(r"[^\s\]]+\.(?:qvd|xls|xlsx|csv|txt|json)", script, flags=re.IGNORECASE))
    lib_suffixes = {ref.split("lib://", 1)[1] for ref in lib_refs if "lib://" in ref}
    file_refs = sorted(
        ref
        for ref in raw_file_refs
        if not any(ref == suffix or ref in suffix or suffix.endswith(ref) for suffix in lib_suffixes)
    )
    drive_paths = sorted(
        set(
            match.strip()
            for match in re.findall(r"[A-Za-z]:\\[^\r\n\0]+", "\n".join(strings))
            if any(ext in match.lower() for ext in (".qvd", ".xls", ".xlsx", ".csv", ".txt", ".json"))
        )
    )
    web_paths = sorted(set(ref for ref in strings if ref.startswith("/media/")))
    return {
        "lib_references": lib_refs,
        "file_references": file_refs,
        "local_paths": drive_paths,
        "media_references": web_paths,
    }


def simplify_sheet(obj: dict[str, Any]) -> dict[str, Any]:
    root = obj.get("qRoot", {})
    prop = root.get("qProperty", {})
    meta = prop.get("qMetaDef", {})
    children = []

    for child in root.get("qChildren", []):
        child_prop = child.get("qProperty", {})
        children.append(
            {
                "id": child_prop.get("qInfo", {}).get("qId"),
                "type": child_prop.get("qInfo", {}).get("qType"),
                "visualization": child_prop.get("visualization"),
                "title": child_prop.get("title"),
                "extends_id": child_prop.get("qExtendsId"),
                "child_count": len(child.get("qChildren", [])),
            }
        )

    return {
        "id": prop.get("qInfo", {}).get("qId"),
        "title": meta.get("title"),
        "description": meta.get("description", ""),
        "thumbnail": prop.get("thumbnail", {}),
        "grid": {
            "columns": prop.get("columns"),
            "rows": prop.get("rows"),
            "grid_mode": prop.get("gridMode"),
            "grid_resolution": prop.get("gridResolution"),
        },
        "cells": prop.get("cells", []),
        "children": children,
        "components": prop.get("components", []),
        "group_id": prop.get("groupId"),
        "layout_options": prop.get("layoutOptions", {}),
    }


def simplify_masterobject(obj: dict[str, Any]) -> dict[str, Any]:
    root = obj.get("qRoot", {})
    prop = root.get("qProperty", {})
    meta = prop.get("qMetaDef", {})
    return {
        "id": prop.get("qInfo", {}).get("qId"),
        "title": meta.get("title"),
        "description": meta.get("description", ""),
        "visualization": prop.get("visualization"),
        "extends_id": prop.get("qExtendsId"),
        "child_count": len(root.get("qChildren", [])),
    }


def simplify_measure(obj: dict[str, Any]) -> dict[str, Any]:
    measure = obj.get("qMeasure", {})
    meta = obj.get("qMetaDef", {})
    return {
        "id": obj.get("qInfo", {}).get("qId"),
        "label": measure.get("qLabel"),
        "title": meta.get("title"),
        "description": meta.get("description", ""),
        "expression": measure.get("qDef"),
        "label_expression": measure.get("qLabelExpression"),
        "number_format": measure.get("qNumFormat"),
        "coloring": measure.get("coloring", {}),
        "tags": meta.get("tags", []),
    }


def simplify_dimension(obj: dict[str, Any]) -> dict[str, Any]:
    dim = obj.get("qDim", {})
    meta = obj.get("qMetaDef", {})
    return {
        "id": obj.get("qInfo", {}).get("qId"),
        "title": meta.get("title"),
        "description": meta.get("description", ""),
        "field_definitions": dim.get("qFieldDefs", []),
        "field_labels": dim.get("qFieldLabels", []),
        "grouping": dim.get("qGrouping"),
        "alias": dim.get("qAlias"),
        "tags": meta.get("tags", []),
        "coloring": dim.get("coloring", {}),
    }


def collect_variables(decoded_objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variables: list[dict[str, Any]] = []
    for record in decoded_objects:
        obj = record["object"]
        if not isinstance(obj, dict) or obj.get("qId") not in {"qvapp_variablelist", "user_variablelist"}:
            continue
        for entry in obj.get("qEntryList", []):
            props = entry.get("qProperties", {})
            variables.append(
                {
                    "id": props.get("qInfo", {}).get("qId"),
                    "name": props.get("qName"),
                    "definition": props.get("qDefinition"),
                    "value": entry.get("qValue"),
                    "is_script_created": entry.get("qIsScriptCreated", False),
                    "number_presentation": props.get("qNumberPresentation"),
                    "tags": props.get("tags", []),
                    "source_list": obj.get("qId"),
                }
            )
    variables.sort(key=lambda item: ((item.get("name") or "").lower(), item.get("id") or ""))
    return variables


def summarise_objects(decoded_objects: list[dict[str, Any]]) -> dict[str, Any]:
    app_data: dict[str, Any] = {}
    script = ""
    data_model_metadata: dict[str, Any] = {}
    app_properties: dict[str, Any] = {}
    load_model: dict[str, Any] = {}
    color_maps: list[dict[str, Any]] = []
    sheets: list[dict[str, Any]] = []
    masterobjects: list[dict[str, Any]] = []
    measures: list[dict[str, Any]] = []
    dimensions: list[dict[str, Any]] = []

    for record in decoded_objects:
        obj = record["object"]
        classification = record["classification"]

        if classification == "app":
            app_data = obj
        elif classification == "script":
            script = obj.get("qScript", "")
        elif classification == "data_model_metadata":
            data_model_metadata = obj
        elif classification == "meta:sheet":
            sheets.append(simplify_sheet(obj))
        elif classification == "meta:masterobject":
            masterobjects.append(simplify_masterobject(obj))
        elif classification == "meta:appprops":
            app_properties = obj
        elif classification == "meta:LoadModel":
            load_model = obj
        elif classification == "meta:ColorMap":
            color_maps.append(obj)
        elif classification == "info:measure":
            measures.append(simplify_measure(obj))
        elif classification == "info:dimension":
            dimensions.append(simplify_dimension(obj))

    sheets.sort(key=lambda item: (item.get("title") or "", item.get("id") or ""))
    masterobjects.sort(key=lambda item: (item.get("title") or "", item.get("id") or ""))
    measures.sort(key=lambda item: (item.get("label") or "", item.get("id") or ""))
    dimensions.sort(key=lambda item: (item.get("title") or "", item.get("id") or ""))

    return {
        "app": app_data,
        "script": script,
        "data_model_metadata": data_model_metadata,
        "app_properties": app_properties,
        "load_model": load_model,
        "color_maps": color_maps,
        "sheets": sheets,
        "masterobjects": masterobjects,
        "measures": measures,
        "dimensions": dimensions,
        "variables": collect_variables(decoded_objects),
    }


def iter_unique_zlib_streams(blob: bytes, known_ranges: list[tuple[int, int]]) -> list[dict[str, Any]]:
    streams: list[dict[str, Any]] = []
    for offset in range(len(blob) - 1):
        if blob[offset] != 0x78 or blob[offset + 1] not in (0x01, 0x5E, 0x9C, 0xDA):
            continue
        within_known_payload = any(start <= offset < end for start, end in known_ranges)
        try:
            stream = zlib.decompressobj()
            output = stream.decompress(blob[offset:])
            consumed = len(blob[offset:]) - len(stream.unused_data)
            if consumed <= 0:
                continue
        except zlib.error:
            continue
        streams.append(
            {
                "offset": offset,
                "compressed_size": consumed,
                "decompressed_size": len(output),
                "within_known_payload": within_known_payload,
                "output": output,
            }
        )
    return streams


def parse_scalar_stream(output: bytes) -> dict[str, Any] | None:
    if len(output) < 8:
        return None
    value_count = int.from_bytes(output[4:8], "little")
    if value_count <= 0:
        return None
    position = 8
    values: list[Any] = []
    tags: list[int] = []

    def decode_int_token(raw: bytes) -> str:
        if raw and all(32 <= byte < 127 for byte in raw):
            return raw.decode("ascii", errors="ignore")
        return str(int.from_bytes(raw, "little"))

    while position < len(output):
        tag = output[position]
        position += 1
        if tag == STRING_TAG:
            if position >= len(output):
                return None
            length = output[position]
            position += 1
            if position + length > len(output):
                return None
            values.append(output[position : position + length].decode("utf-8", errors="ignore"))
            position += length
            tags.append(tag)
        elif tag == DOUBLE_TAG:
            if position + 8 > len(output):
                return None
            values.append(struct.unpack("<d", output[position : position + 8])[0])
            position += 8
            tags.append(tag)
        elif tag == INTEGER_TAG:
            if position >= len(output):
                return None
            length = output[position]
            position += 1
            if position + length > len(output):
                return None
            raw = output[position : position + length]
            position += length
            if position + 4 <= len(output) and (position + 4 == len(output) or output[position + 4] in {STRING_TAG, DOUBLE_TAG, INTEGER_TAG, 6}):
                numeric = int.from_bytes(output[position : position + 4], "little")
                position += 4
                values.append((tag, raw, numeric))
            else:
                values.append((tag, raw))
            tags.append(tag)
        elif tag == 1:
            if position + 4 > len(output):
                return None
            numeric = int.from_bytes(output[position : position + 4], "little")
            position += 4
            values.append((tag, numeric))
            tags.append(tag)
        elif tag == 6:
            if position >= len(output):
                return None
            length = output[position]
            position += 1
            if position + length + 8 > len(output):
                return None
            text = output[position : position + length].decode("utf-8", errors="ignore")
            position += length
            numeric = struct.unpack("<d", output[position : position + 8])[0]
            position += 8
            values.append((tag, text, numeric))
            tags.append(tag)
        else:
            return None

    if position != len(output) or len(values) != value_count:
        return None

    tag_set = set(tags)
    if tag_set.issubset({STRING_TAG, INTEGER_TAG, 1}) and STRING_TAG in tag_set:
        normalized: list[str] = []
        for value in values:
            if isinstance(value, tuple) and len(value) == 3:
                _tag, raw, numeric = value
                token = raw.decode("ascii", errors="ignore") if raw and all(32 <= byte < 127 for byte in raw) else str(numeric)
                normalized.append(token)
            elif isinstance(value, tuple) and len(value) == 2 and value[0] == 1:
                _tag, numeric = value
                normalized.append(str(numeric))
            elif isinstance(value, tuple):
                _tag, raw = value
                normalized.append(decode_int_token(raw))
            else:
                normalized.append(str(value))
        return {
            "count": value_count,
            "value_type": "str",
            "values": normalized,
        }

    if tag_set.issubset({DOUBLE_TAG, INTEGER_TAG, 6, 1}):
        normalized_numeric: list[float | int] = []
        for value in values:
            if isinstance(value, tuple) and len(value) == 3:
                _tag, _raw, numeric = value
                normalized_numeric.append(numeric)
            elif isinstance(value, tuple) and len(value) == 2 and value[0] == 1:
                _tag, numeric = value
                normalized_numeric.append(numeric)
            elif isinstance(value, tuple) and len(value) == 2:
                _tag, raw = value
                normalized_numeric.append(int.from_bytes(raw, "little"))
            else:
                normalized_numeric.append(value)

        if all(abs(float(item) - round(float(item))) < 1e-9 for item in normalized_numeric):
            return {
                "count": value_count,
                "value_type": "int",
                "values": [int(round(float(item))) for item in normalized_numeric],
            }
        return {
            "count": value_count,
            "value_type": "double",
            "values": [float(item) for item in normalized_numeric],
        }

    return None


def infer_scalar_traits(values: list[Any], value_type: str) -> dict[str, Any]:
    traits: dict[str, Any] = {}
    if value_type == "str":
        traits["unique_count"] = len({value for value in values})
        traits["all_upper_alpha"] = all(isinstance(value, str) and value.isalpha() and value.upper() == value for value in values)
        traits["all_currency_symbols"] = all(isinstance(value, str) and len(value) <= 2 for value in values)
        traits["all_hex_colors"] = all(isinstance(value, str) and re.fullmatch(r"#[0-9A-Fa-f]{6}", value) for value in values)
        traits["average_length"] = sum(len(value) for value in values) / len(values)
        traits["space_ratio"] = sum(" " in value for value in values) / len(values)
        traits["all_hyphenated"] = all(isinstance(value, str) and "-" in value for value in values)
        traits["contains_company_words"] = any(
            re.search(r"\b(Inc|Corp|Corporation|Ltd|Fund|Plan|Trust|Bank)\b", value) for value in values
        )
        traits["contains_sector_words"] = all(len(value) < 32 and "," not in value for value in values)
    else:
        numeric_values = [float(value) for value in values]
        traits["unique_count"] = len({value for value in numeric_values})
        traits["min"] = min(numeric_values)
        traits["max"] = max(numeric_values)
        traits["all_integral"] = all(abs(value - round(value)) < 1e-9 for value in numeric_values)
        traits["is_monotonic_non_decreasing"] = all(
            numeric_values[index] <= numeric_values[index + 1] for index in range(len(numeric_values) - 1)
        )
        traits["average_abs"] = sum(abs(value) for value in numeric_values) / len(numeric_values)
    return traits


def discover_scalar_streams(blob: bytes, format_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    known_ranges = [(block["payload_offset"], block["payload_end"]) for block in format_blocks]
    scalar_streams: list[dict[str, Any]] = []
    for stream in iter_unique_zlib_streams(blob, known_ranges):
        if stream["within_known_payload"]:
            continue
        parsed = parse_scalar_stream(stream["output"])
        if not parsed:
            continue
        scalar_streams.append(
            {
                **stream,
                **parsed,
                "traits": infer_scalar_traits(parsed["values"], parsed["value_type"]),
            }
        )
    return scalar_streams


def classify_non_scalar_streams(
    blob: bytes,
    format_blocks: list[dict[str, Any]],
    table_specs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    known_ranges = [(block["payload_offset"], block["payload_end"]) for block in format_blocks]
    rows: list[dict[str, Any]] = []

    for stream in iter_unique_zlib_streams(blob, known_ranges):
        if stream["within_known_payload"] or parse_scalar_stream(stream["output"]):
            continue

        output = stream["output"]
        if not output:
            continue

        distinct_byte_count = len(set(output[: min(len(output), 4096)]))
        zero_ratio = round(output.count(0) / len(output), 6)
        candidate_tables: list[dict[str, Any]] = []

        for spec in table_specs:
            row_count = spec["row_count"]
            if row_count <= 0:
                continue
            for width in (1, 2, 4, 8, 16):
                if abs(len(output) - (row_count * width)) <= max(16, width * 2):
                    candidate_tables.append(
                        {
                            "table": spec["name"],
                            "row_count": row_count,
                            "candidate_width": width,
                            "reason": "decompressed_size_close_to_row_count_times_width",
                        }
                    )
                    break

        if zero_ratio >= 0.55 and len(output) >= 1024:
            classification = "row_or_null_layout_candidate"
        elif distinct_byte_count <= 32 and len(output) >= 256:
            classification = "dictionary_index_vector_candidate"
        elif len(output) >= 1024:
            classification = "numeric_or_packed_vector_candidate"
        else:
            classification = "unresolved_binary"

        rows.append(
            {
                "offset": stream["offset"],
                "compressed_size": stream["compressed_size"],
                "decompressed_size": len(output),
                "classification": classification,
                "distinct_byte_count": distinct_byte_count,
                "zero_ratio": zero_ratio,
                "preview_hex": output[:32].hex(),
                "preview_ascii": printable_window(output[:64]),
                "candidate_tables": candidate_tables,
            }
        )

    return rows


def discover_index_vectors(
    blob: bytes,
    format_blocks: list[dict[str, Any]],
    table_specs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    known_ranges = [(block["payload_offset"], block["payload_end"]) for block in format_blocks]
    large_specs = [spec for spec in table_specs if spec["row_count"] >= 100]
    vectors: list[dict[str, Any]] = []
    seen_signatures: set[tuple[int, str, int, int]] = set()

    def unpack_lsb_values(payload: bytes, header_bytes: int, bits_per_value: int, count: int) -> list[int]:
        data = payload[header_bytes:]
        mask = (1 << bits_per_value) - 1
        values: list[int] = []
        bit_offset = 0
        for _ in range(count):
            byte_index = bit_offset // 8
            shift = bit_offset % 8
            chunk = int.from_bytes(data[byte_index : byte_index + 4], "little")
            values.append((chunk >> shift) & mask)
            bit_offset += bits_per_value
        return values

    def vector_cardinality_distance(unique_count: int, max_value: int, cardinality: int) -> int:
        candidates = [
            abs(unique_count - cardinality),
            abs((unique_count - 1) - cardinality),
            abs(max_value - cardinality),
            abs((max_value - 1) - cardinality),
            abs((max_value + 1) - cardinality),
        ]
        return min(candidates)

    for stream in iter_unique_zlib_streams(blob, known_ranges):
        if stream["within_known_payload"] or parse_scalar_stream(stream["output"]):
            continue

        output = stream["output"]
        for spec in large_specs:
            row_count = spec["row_count"]
            field_cardinalities = [int(field.get("qcardinal") or 0) for field in spec["fields"] if int(field.get("qcardinal") or 0) > 0]
            if not field_cardinalities:
                continue
            discovery_ratio = 0.25 if any(is_key_field(field) for field in spec["fields"]) else 0.15

            for bits_per_value in range(1, 17):
                data_bytes = (row_count * bits_per_value + 7) // 8
                header_bytes = len(output) - data_bytes
                if header_bytes < 0 or header_bytes > 16:
                    continue

                signature = (stream["offset"], spec["name"], bits_per_value, header_bytes)
                if signature in seen_signatures:
                    continue

                values = unpack_lsb_values(output, header_bytes, bits_per_value, row_count)
                unique_values = sorted(set(values))
                max_value = max(unique_values)
                unique_count = len(unique_values)
                best_cardinality = min(field_cardinalities, key=lambda item: vector_cardinality_distance(unique_count, max_value, item))
                best_distance = vector_cardinality_distance(unique_count, max_value, best_cardinality)
                discovery_threshold = max(1, int(best_cardinality * discovery_ratio))

                if best_distance > discovery_threshold:
                    continue

                seen_signatures.add(signature)
                vectors.append(
                    {
                        "offset": stream["offset"],
                        "table": spec["name"],
                        "row_count": row_count,
                        "bits_per_value": bits_per_value,
                        "header_bytes": header_bytes,
                        "values": values,
                        "min": min(unique_values),
                        "max": max_value,
                        "unique_count": unique_count,
                        "unique_values": unique_values,
                        "best_cardinality": best_cardinality,
                        "best_distance": best_distance,
                    }
                )

    return vectors


def build_block_index(
    format_blocks: list[dict[str, Any]], blob: bytes
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    known_ranges = [(block["payload_offset"], block["payload_end"]) for block in format_blocks]
    block_rows: list[dict[str, Any]] = []
    opaque_rows: list[dict[str, Any]] = []

    for block in format_blocks:
        block_rows.append(
            {
                "block_type": "format_record",
                "marker_offset": block["marker_offset"],
                "payload_offset": block["payload_offset"],
                "payload_end": block["payload_end"],
                "format": block["format"],
                "classification": block.get("classification"),
                "declared_size": block.get("declared_size"),
                "stored_size": block.get("stored_size"),
                "decompressed_size": block.get("decompressed_size"),
                "status": block.get("status"),
                "header_type": block.get("header", {}).get("type"),
                "content_hash": block.get("header", {}).get("content_hash"),
            }
        )
        if block.get("status") == "decompression_failed":
            opaque_rows.append(
                {
                    "offset": block["marker_offset"],
                    "kind": "format_record",
                    "format": block["format"],
                    "classification": block.get("classification"),
                    "reason": block.get("error"),
                }
            )

    for stream in iter_unique_zlib_streams(blob, known_ranges):
        block_rows.append(
            {
                "block_type": "zlib_stream",
                "offset": stream["offset"],
                "compressed_size": stream["compressed_size"],
                "decompressed_size": stream["decompressed_size"],
                "within_known_payload": stream["within_known_payload"],
                "preview": stream["output"][:120].decode("utf-8", errors="ignore"),
            }
        )

    return block_rows, opaque_rows


def parse_inline_table(script: str, table_name: str) -> list[dict[str, str]]:
    pattern = re.compile(
        rf"{re.escape(table_name)}:\s*Load\s+\*\s+Inline\s+\[(.*?)\];",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(script)
    if not match:
        return []
    body = match.group(1).strip()
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    if len(lines) < 2:
        return []
    headers = [part.strip() for part in lines[0].split(",")]
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        parts = [part.strip() for part in line.split(",", maxsplit=len(headers) - 1)]
        if len(parts) != len(headers):
            continue
        row: dict[str, str] = {}
        for header, value in zip(headers, parts):
            cleaned = value.strip()
            if cleaned.startswith("'") and cleaned.endswith("'"):
                cleaned = cleaned[1:-1]
            row[header] = cleaned
        rows.append(row)
    return rows


def is_numeric_field(field: dict[str, Any]) -> bool:
    return bool(field.get("qis_numeric"))


def is_integer_field(field: dict[str, Any]) -> bool:
    return "$integer" in (field.get("qtags") or [])


def is_key_field(field: dict[str, Any]) -> bool:
    return "$key" in (field.get("qtags") or []) or bool(field.get("qdistinct_only"))


def is_numeric_identifier_key_field(field: dict[str, Any]) -> bool:
    field_name = (field.get("qname") or "").lower()
    if "cusip" in field_name or "code" in field_name or field_name == "%key":
        return False
    return is_key_field(field) and is_numeric_field(field) and ("id" in field_name or "$integer" in (field.get("qtags") or []))


def field_value_kind(field: dict[str, Any]) -> str:
    if is_numeric_identifier_key_field(field):
        return "numeric"
    if is_key_field(field):
        return "text"
    return "numeric" if is_numeric_field(field) else "text"


def scalar_stream_matches_field(field: dict[str, Any], stream: dict[str, Any]) -> bool:
    kind = field_value_kind(field)
    if kind == "numeric":
        return stream["value_type"] in {"double", "int"}
    return stream["value_type"] == "str"


def normalize_stream_values(field: dict[str, Any], values: list[Any]) -> list[Any]:
    if field_value_kind(field) != "numeric":
        return [str(value) if value is not None else None for value in values]

    normalized: list[Any] = []
    for value in values:
        if value is None:
            normalized.append(None)
            continue
        numeric = float(value)
        if is_integer_field(field) and abs(numeric - round(numeric)) < 1e-9:
            normalized.append(int(round(numeric)))
        else:
            normalized.append(numeric)
    return normalized


def build_table_specs(data_model: dict[str, Any]) -> list[dict[str, Any]]:
    qfields = data_model.get("qfields", [])
    specs: list[dict[str, Any]] = []
    for table in data_model.get("qtables", []):
        table_name = table.get("qname")
        fields = [field for field in qfields if table_name in field.get("qsrc_tables", [])]
        specs.append(
            {
                "name": table_name,
                "row_count": int(table.get("qno_of_rows") or 0),
                "fields": fields,
            }
        )
    return specs


def default_column_confidence(field: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "missing",
        "coverage_ratio": 0.0,
        "inferred_type": field_value_kind(field),
        "notes": "No row-aligned reconstruction found yet.",
        "source_block_offsets": [],
        "candidate_dictionary_offsets": [],
    }


def create_empty_table_state(spec: dict[str, Any]) -> dict[str, Any]:
    columns = {field["qname"]: [None] * spec["row_count"] for field in spec["fields"]}
    confidence = {field["qname"]: default_column_confidence(field) for field in spec["fields"]}
    mapping = {field["qname"]: [] for field in spec["fields"]}
    return {"columns": columns, "confidence": confidence, "mapping": mapping}


def set_column(
    table_state: dict[str, Any],
    field: dict[str, Any],
    values: list[Any],
    status: str,
    notes: str,
    source_offsets: list[int],
    method: str,
) -> None:
    field_name = field["qname"]
    column = table_state["columns"][field_name]
    limit = min(len(column), len(values))
    normalized = normalize_stream_values(field, values)
    for index in range(limit):
        column[index] = normalized[index]

    coverage = sum(value is not None for value in column) / len(column) if column else 0.0
    table_state["confidence"][field_name] = {
        "status": status,
        "coverage_ratio": round(coverage, 6),
        "inferred_type": field_value_kind(field),
        "notes": notes,
        "source_block_offsets": source_offsets,
        "candidate_dictionary_offsets": table_state["confidence"][field_name].get("candidate_dictionary_offsets", []),
    }
    table_state["mapping"][field_name].append(
        {
            "method": method,
            "source_block_offsets": source_offsets,
            "status": status,
            "notes": notes,
        }
    )


def add_candidate_offsets(table_state: dict[str, Any], field_name: str, offsets: list[int]) -> None:
    entry = table_state["confidence"][field_name]
    existing = entry.get("candidate_dictionary_offsets", [])
    entry["candidate_dictionary_offsets"] = sorted(set(existing + offsets))


def find_streams(
    scalar_streams: list[dict[str, Any]],
    *,
    count: int | None = None,
    value_type: str | None = None,
    predicate: Any = None,
) -> list[dict[str, Any]]:
    streams = scalar_streams
    if count is not None:
        streams = [stream for stream in streams if stream["count"] == count]
    if value_type is not None:
        streams = [stream for stream in streams if stream["value_type"] == value_type]
    if predicate is not None:
        streams = [stream for stream in streams if predicate(stream)]
    return sorted(streams, key=lambda stream: stream["offset"])


def looks_like_currency_codes(stream: dict[str, Any]) -> bool:
    return stream["value_type"] == "str" and stream["traits"].get("all_upper_alpha") and all(
        len(value) == 3 for value in stream["values"]
    )


def looks_like_currency_symbols(stream: dict[str, Any]) -> bool:
    return stream["value_type"] == "str" and stream["traits"].get("all_currency_symbols") and any(
        value in {"$", "€", "£"} for value in stream["values"]
    )


def looks_like_hex_colors(stream: dict[str, Any]) -> bool:
    return stream["value_type"] == "str" and stream["traits"].get("all_hex_colors")


def looks_like_holdings_names(stream: dict[str, Any]) -> bool:
    return stream["value_type"] == "str" and stream["count"] == 10 and (
        stream["traits"].get("contains_company_words") or stream["traits"].get("average_length", 0) > 20
    )


def looks_like_sector_names(stream: dict[str, Any]) -> bool:
    return stream["value_type"] == "str" and stream["count"] == 10 and stream["traits"].get("contains_sector_words") and not stream["traits"].get("contains_company_words")


def looks_like_code_dictionary(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = [str(value) for value in stream["values"] if value is not None]
    if not values:
        return False
    pattern = re.compile(r"[A-Z0-9][A-Z0-9_./-]{1,15}$")
    match_ratio = sum(bool(pattern.fullmatch(value)) for value in values) / len(values)
    return match_ratio >= 0.8 and stream["traits"].get("space_ratio", 1.0) <= 0.15


def looks_like_org_code_dictionary(stream: dict[str, Any]) -> bool:
    return looks_like_code_dictionary(stream) and any("_" in str(value) or len(str(value)) > 6 for value in stream["values"])


def looks_like_people_names(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = [str(value) for value in stream["values"] if value]
    if not values:
        return False
    spaced_ratio = sum(" " in value or "," in value for value in values) / len(values)
    return spaced_ratio >= 0.6 and stream["traits"].get("average_length", 0) >= 8


def looks_like_metric_labels(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    metric_terms = {"Alpha", "Beta", "R-squared", "Sharpe ratio", "Mean annual return", "Standard deviation"}
    values = {str(value) for value in stream["values"] if value}
    return bool(values) and values.issubset(metric_terms)


def looks_like_trade_source_codes(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = {str(value) for value in stream["values"] if value}
    return bool(values) and values.issubset({"CRD", "TB", "INTERNAL_TB"})


def looks_like_trade_status_values(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = {str(value).upper() for value in stream["values"] if value}
    allowed = {"CREATE", "CANCEL", "AMEND", "ALLOCATED", "MATCHED", "SETTLED", "PENDING", "BOOKED", "CONFIRMED", "FAILED"}
    return bool(values) and values.issubset(allowed)


def looks_like_buy_sell_values(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = {str(value) for value in stream["values"] if value}
    return bool(values) and values.issubset({"Buy", "Sell"})


def looks_like_exempt_status(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = {str(value) for value in stream["values"] if value}
    return bool(values) and values.issubset({"Exempt", "Non-Exempt"})


def looks_like_client_type(stream: dict[str, Any]) -> bool:
    if stream["value_type"] != "str":
        return False
    values = {str(value) for value in stream["values"] if value}
    return bool(values) and values.issubset({"Institutional", "Retail"})


def looks_like_date_serials(stream: dict[str, Any]) -> bool:
    if stream["value_type"] not in {"double", "int"}:
        return False
    traits = stream["traits"]
    return (
        traits.get("all_integral")
        and 30000 <= traits.get("min", 0) <= 60000
        and 30000 <= traits.get("max", 0) <= 60000
    )


def looks_like_rowcount_markers(stream: dict[str, Any]) -> bool:
    if stream["value_type"] not in {"double", "int"}:
        return False
    traits = stream["traits"]
    return traits.get("all_integral") and stream["count"] <= 16 and traits.get("max", 0) >= 1000 and not looks_like_date_serials(stream)


def dictionary_vector_proximity_bonus(dictionary_stream: dict[str, Any], vector: dict[str, Any]) -> int:
    return max(0, 40 - abs(vector["offset"] - dictionary_stream["offset"]) // 100000)


def looks_like_internal_table_names(stream: dict[str, Any], known_table_names: set[str] | None) -> bool:
    if stream["value_type"] != "str" or not known_table_names:
        return False
    values = {str(value).strip().lower() for value in stream["values"] if value}
    if not values:
        return False
    normalized_table_names = {name.strip().lower() for name in known_table_names if name}
    overlap = values & normalized_table_names
    return len(overlap) / len(values) >= 0.7


def string_semantic_bonus(field: dict[str, Any], stream: dict[str, Any], known_table_names: set[str] | None = None) -> int:
    field_name = (field.get("qname") or "").lower()
    bonus = 0

    if "currency" in field_name:
        if looks_like_currency_codes(stream):
            bonus += 220
        elif looks_like_currency_symbols(stream):
            bonus += 40
        else:
            bonus -= 140

    if "symbol" in field_name:
        if looks_like_currency_symbols(stream):
            bonus += 220
        elif looks_like_currency_codes(stream):
            bonus += 40
        else:
            bonus -= 60

    if field_name == "tax status":
        if looks_like_exempt_status(stream):
            bonus += 220
        if looks_like_client_type(stream):
            bonus -= 180

    if field_name == "client type":
        if looks_like_client_type(stream):
            bonus += 220
        if looks_like_exempt_status(stream):
            bonus -= 180

    if field_name == "trade source":
        if looks_like_trade_source_codes(stream):
            bonus += 260
        if looks_like_metric_labels(stream):
            bonus -= 220

    if field_name in {"tradestatus", "trade status"}:
        if looks_like_trade_status_values(stream):
            bonus += 260
        if looks_like_exempt_status(stream) or looks_like_client_type(stream) or looks_like_buy_sell_values(stream):
            bonus -= 220

    if field_name == "referral source":
        if looks_like_internal_table_names(stream, known_table_names):
            bonus -= 320
        if looks_like_code_dictionary(stream):
            bonus -= 140
        elif stream["traits"].get("space_ratio", 0.0) <= 0.1 and stream["traits"].get("average_length", 0) <= 8:
            bonus += 60

    if "transactiontype" in field_name or field_name == "transaction type":
        if looks_like_buy_sell_values(stream):
            bonus += 260
        if looks_like_client_type(stream) or looks_like_exempt_status(stream):
            bonus -= 220

    if "fund code" in field_name or field_name == "%key" or "cusip" in field_name or field_name == "brokercode":
        if looks_like_code_dictionary(stream):
            bonus += 180
            if "org" in field_name and looks_like_org_code_dictionary(stream):
                bonus += 40
            if "org" not in field_name and looks_like_org_code_dictionary(stream):
                bonus -= 20
        else:
            bonus -= 140

    if "manager" in field_name and looks_like_people_names(stream):
        bonus += 180

    if field_name in {"investor", "account name", "fund name", "original fund name", "pooled fund (if applicable)", "securityname", "security name"}:
        if stream["traits"].get("contains_company_words") or looks_like_people_names(stream) or stream["traits"].get("average_length", 0) >= 10:
            bonus += 90

    if "location" in field_name:
        if looks_like_metric_labels(stream):
            bonus -= 200
        if looks_like_code_dictionary(stream):
            bonus += 60

    if "desk" in field_name and looks_like_code_dictionary(stream):
        bonus -= 80

    return bonus


def numeric_semantic_bonus(field: dict[str, Any], stream: dict[str, Any]) -> int:
    field_name = (field.get("qname") or "").lower()
    traits = stream["traits"]
    bonus = 0

    if "date" in field_name or "period" in field_name:
        if looks_like_date_serials(stream):
            bonus += 240
        if looks_like_rowcount_markers(stream):
            bonus -= 260

    if "price" in field_name:
        if 0 <= traits.get("average_abs", 0) < 100000 and traits.get("max", 0) < 1000000:
            bonus += 120
        if looks_like_date_serials(stream):
            bonus -= 220
        if looks_like_rowcount_markers(stream):
            bonus -= 260

    if "quantity" in field_name:
        if traits.get("all_integral"):
            bonus += 50
        if looks_like_rowcount_markers(stream):
            bonus -= 180

    if "amount" in field_name or field_name == "value":
        if traits.get("average_abs", 0) > 1000:
            bonus += 60
        if looks_like_rowcount_markers(stream):
            bonus -= 180

    if field_name.endswith("id") or field_name == "performanceid" or field_name == "trade id":
        if traits.get("all_integral"):
            bonus += 120

    return bonus


def choose_closest_stream(reference_offset: int, streams: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not streams:
        return None
    return min(streams, key=lambda stream: abs(stream["offset"] - reference_offset))


def choose_closest_stream_pair(
    left_streams: list[dict[str, Any]],
    right_streams: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    if not left_streams or not right_streams:
        return None, None
    return min(
        ((left, right) for left in left_streams for right in right_streams),
        key=lambda pair: (
            abs(pair[0]["offset"] - pair[1]["offset"]),
            max(pair[0]["offset"], pair[1]["offset"]),
            min(pair[0]["offset"], pair[1]["offset"]),
        ),
    )


def apply_small_table_reconstruction(
    spec: dict[str, Any],
    table_state: dict[str, Any],
    scalar_streams: list[dict[str, Any]],
    script: str,
) -> set[int]:
    used_offsets: set[int] = set()
    fields = {field["qname"]: field for field in spec["fields"]}

    if spec["name"] == "Currency" and fields:
        codes = find_streams(scalar_streams, count=spec["row_count"], value_type="str", predicate=looks_like_currency_codes)
        symbols = find_streams(scalar_streams, count=spec["row_count"], value_type="str", predicate=looks_like_currency_symbols)
        code_stream, symbol_stream = choose_closest_stream_pair(codes, symbols)
        if code_stream and symbol_stream:
            set_column(table_state, fields["_CURRENCY"], code_stream["values"], "exact", "Exact three-code currency dictionary decoded from the closest matching scalar stream pair.", [code_stream["offset"]], "scalar_dictionary")
            set_column(table_state, fields["_SYMBOL"], symbol_stream["values"], "exact", "Exact currency symbol dictionary decoded from the closest matching scalar stream pair.", [symbol_stream["offset"]], "scalar_dictionary")
            used_offsets.update({code_stream["offset"], symbol_stream["offset"]})
        return used_offsets

    if spec["name"] == "Colors" and fields:
        inline_rows = parse_inline_table(script, spec["name"])
        if inline_rows:
            set_column(
                table_state,
                fields["Asset Class"],
                [row.get("Asset Class") for row in inline_rows],
                "exact",
                "Exact inline table decoded from the load script.",
                [],
                "script_inline",
            )
            set_column(
                table_state,
                fields["Color"],
                [row.get("Color") for row in inline_rows],
                "exact",
                "Exact inline table decoded from the load script.",
                [],
                "script_inline",
            )
            return used_offsets
        asset_classes = find_streams(scalar_streams, count=4, value_type="str", predicate=lambda stream: stream["values"] == ["Equity", "Fixed Income", "Alternatives", "Multi-Asset"])
        colors = find_streams(scalar_streams, count=4, value_type="str", predicate=looks_like_hex_colors)
        if asset_classes and colors:
            set_column(table_state, fields["Asset Class"], asset_classes[0]["values"], "exact", "Exact asset-class dictionary decoded from a scalar stream.", [asset_classes[0]["offset"]], "scalar_dictionary")
            set_column(table_state, fields["Color"], colors[0]["values"], "exact", "Exact color dictionary decoded from a scalar stream.", [colors[0]["offset"]], "scalar_dictionary")
            used_offsets.update({asset_classes[0]["offset"], colors[0]["offset"]})
        return used_offsets

    if spec["name"] == "Holdings" and fields:
        names = find_streams(scalar_streams, count=10, value_type="str", predicate=looks_like_holdings_names)
        if names:
            name_stream = names[0]
            set_column(table_state, fields["Holdings"], name_stream["values"], "exact", "Exact 10-row holdings dictionary decoded from a scalar stream.", [name_stream["offset"]], "scalar_dictionary")
            used_offsets.add(name_stream["offset"])
            numeric_candidates = [stream for stream in find_streams(scalar_streams, count=9, value_type="double") if stream["offset"] not in used_offsets]
            ytd_stream = choose_closest_stream(name_stream["offset"], numeric_candidates)
            if ytd_stream:
                padded = ytd_stream["values"] + [None] * max(0, spec["row_count"] - len(ytd_stream["values"]))
                set_column(
                    table_state,
                    fields["YTD"],
                    padded,
                    "partial",
                    "Nine unique numeric values were recovered for a 10-row table. Values are emitted in discovered order and padded with null where row alignment remains unresolved.",
                    [ytd_stream["offset"]],
                    "nearest_numeric_dictionary",
                )
                used_offsets.add(ytd_stream["offset"])
        return used_offsets

    if spec["name"] == "Composition" and fields:
        sectors = find_streams(scalar_streams, count=10, value_type="str", predicate=looks_like_sector_names)
        if sectors:
            sector_stream = sectors[0]
            set_column(table_state, fields["Sector"], sector_stream["values"], "exact", "Exact 10-row sector dictionary decoded from a scalar stream.", [sector_stream["offset"]], "scalar_dictionary")
            used_offsets.add(sector_stream["offset"])
            numeric_candidates = [stream for stream in find_streams(scalar_streams, count=9, value_type="double") if stream["offset"] not in used_offsets]
            nearest = sorted(numeric_candidates, key=lambda stream: abs(stream["offset"] - sector_stream["offset"]))[:2]
            if len(nearest) == 2:
                for field_name, stream in zip(["Sector YTD", "Sector LYTD"], sorted(nearest, key=lambda item: item["offset"])):
                    padded = stream["values"] + [None] * max(0, spec["row_count"] - len(stream["values"]))
                    set_column(
                        table_state,
                        fields[field_name],
                        padded,
                        "partial",
                        "Nine unique numeric values were recovered for a 10-row table. Values are emitted in discovered order and padded with null where row alignment remains unresolved.",
                        [stream["offset"]],
                        "nearest_numeric_dictionary",
                    )
                    used_offsets.add(stream["offset"])
        return used_offsets

    return used_offsets


def choose_constant_scalar_value(field: dict[str, Any], scalar_streams: list[dict[str, Any]]) -> Any | None:
    kind = field_value_kind(field)
    constant_streams = [stream for stream in scalar_streams if stream["count"] == 1 and scalar_stream_matches_field(field, stream)]
    if kind == "text":
        preferred = [stream for stream in constant_streams if stream["values"] and str(stream["values"][0]).strip()]
        if field["qname"] in {"Trade Trader", "Trade Customer"}:
            hgi_streams = [stream for stream in preferred if str(stream["values"][0]).strip().upper() == "HGI"]
            if hgi_streams:
                return hgi_streams[0]["values"][0]
        if preferred:
            return preferred[0]["values"][0]
        if constant_streams:
            return constant_streams[0]["values"][0]
        return None

    if field["qname"] == "PriceAsPercent":
        return 0
    if constant_streams:
        return constant_streams[0]["values"][0]
    return None


def apply_constant_field_reconstruction(
    spec: dict[str, Any],
    table_state: dict[str, Any],
    scalar_streams: list[dict[str, Any]],
    used_offsets: set[int],
) -> None:
    for field in spec["fields"]:
        field_name = field["qname"]
        if table_state["confidence"][field_name]["status"] != "missing":
            continue
        if int(field.get("qcardinal") or 0) == 0:
            set_column(
                table_state,
                field,
                [None] * spec["row_count"],
                "exact",
                "The field has no stored values in the QVF metadata, so it is exported as a null-filled column.",
                [],
                "null_column",
            )
            continue
        if int(field.get("qcardinal") or 0) != 1:
            continue

        constant_value = choose_constant_scalar_value(field, scalar_streams)
        if constant_value is None:
            continue

        values = [constant_value] * spec["row_count"]
        notes = "A single-value dictionary stream was expanded across the table."
        status = "exact"
        if field_value_kind(field) == "numeric" and field_name == "PriceAsPercent":
            notes = "No dedicated scalar stream was present, so the single-valued numeric column was expanded as a constant zero."
            status = "heuristic"

        source_offsets = [stream["offset"] for stream in scalar_streams if stream["count"] == 1 and scalar_stream_matches_field(field, stream)]
        set_column(table_state, field, values, status, notes, source_offsets[:1], "constant_dictionary")
        if source_offsets:
            used_offsets.update(source_offsets[:1])


def score_relaxed_vector_assignment(
    field: dict[str, Any],
    dictionary_stream: dict[str, Any],
    vector: dict[str, Any],
    known_table_names: set[str] | None = None,
) -> tuple[int, str | None]:
    kind = field_value_kind(field)
    dictionary_count = int(field.get("qcardinal") or 0)

    if kind == "numeric" and dictionary_stream["value_type"] not in {"double", "int"}:
        return -1_000_000, None
    if kind == "text" and dictionary_stream["value_type"] != "str":
        return -1_000_000, None

    prefer_no_nulls = int(field.get("qtotal_count") or 0) == vector["row_count"] and vector["row_count"] > 0
    schemes = candidate_index_mapping_schemes(vector, dictionary_count, prefer_no_nulls=prefer_no_nulls)
    if kind == "text" and "ranked_unique" not in schemes and vector["unique_count"] <= max(3, dictionary_count * 2):
        schemes.append("ranked_unique")
    if not schemes:
        return -1_000_000, None
    best_scheme = None
    best_scheme_score = -1_000_000
    for scheme in schemes:
        quality_score = scheme_quality_score(field, vector, dictionary_stream["values"], scheme)
        if quality_score > best_scheme_score:
            best_scheme = scheme
            best_scheme_score = quality_score
    scheme = best_scheme
    if not scheme:
        return -1_000_000, None

    dictionary_distance = min(
        abs(vector["unique_count"] - dictionary_count),
        abs((vector["unique_count"] - 1) - dictionary_count),
        abs(vector["max"] - dictionary_count),
        abs((vector["max"] - 1) - dictionary_count),
        abs((vector["max"] + 1) - dictionary_count),
    )
    max_distance = max(1, int(dictionary_count * (0.35 if is_key_field(field) else 0.25)))
    if dictionary_distance > max_distance:
        return -1_000_000, None

    score = 400
    score -= dictionary_distance * (2 if is_key_field(field) else 5)
    if vector["best_distance"] == 0:
        score += 60
    if vector["unique_count"] == dictionary_count:
        score += 50
    elif vector["unique_count"] <= dictionary_count:
        score += 20
    elif vector["unique_count"] <= dictionary_count * 2:
        score += 10
    if vector["max"] == dictionary_count - 1:
        score += 25
    elif vector["max"] == dictionary_count:
        score += 15
    elif vector["max"] == dictionary_count + 1:
        score += 10
    if vector["bits_per_value"] in {8, 16}:
        score += 10

    field_name = field["qname"].lower()
    if kind == "text" and ("code" in field_name or "key" in field_name):
        score += 10
    if kind == "numeric" and any(token in field_name for token in ["amount", "price", "value", "quantity", "percent", "year"]):
        score += 10
    if kind == "text":
        score += string_semantic_bonus(field, dictionary_stream, known_table_names=known_table_names)
    else:
        score += numeric_semantic_bonus(field, dictionary_stream)
    score += best_scheme_score

    return score, scheme


def apply_relaxed_field_reconstruction(
    spec: dict[str, Any],
    table_state: dict[str, Any],
    scalar_streams: list[dict[str, Any]],
    index_vectors: list[dict[str, Any]],
    used_offsets: set[int],
    known_table_names: set[str] | None = None,
) -> None:
    table_vectors = [vector for vector in index_vectors if vector["table"] == spec["name"]]

    for field in spec["fields"]:
        field_name = field["qname"]
        if table_state["confidence"][field_name]["status"] != "missing":
            continue

        field_kind = field_value_kind(field)
        dictionary_streams = [
            stream
            for stream in scalar_streams
            if stream["count"] == int(field.get("qcardinal") or 0) and scalar_stream_matches_field(field, stream)
        ]
        add_candidate_offsets(table_state, field_name, [stream["offset"] for stream in dictionary_streams])

        direct_numeric_candidate = None
        if field_kind == "numeric" and not dictionary_streams:
            exact_vectors = [
                vector
                for vector in table_vectors
                if vector["unique_count"] == int(field.get("qcardinal") or 0)
                and vector["best_distance"] == 0
                and vector["max"] == int(field.get("qcardinal") or 0) - 1
            ]
            if exact_vectors:
                direct_numeric_candidate = min(exact_vectors, key=lambda item: (item["best_distance"], item["offset"]))

        if direct_numeric_candidate:
            values = direct_numeric_candidate["values"]
            notes = (
                "Direct packed numeric values were recovered from a zlib vector with an exact cardinality match. "
                f"Source offset {direct_numeric_candidate['offset']}."
            )
            set_column(
                table_state,
                field,
                values,
                "exact",
                notes,
                [direct_numeric_candidate["offset"]],
                "direct_numeric_vector",
            )
            continue

        if field_kind == "numeric" and not dictionary_streams:
            relaxed_numeric_candidates = sorted(
                table_vectors,
                key=lambda item: (
                    item["best_distance"],
                    abs(item["unique_count"] - int(field.get("qcardinal") or 0)),
                    item["offset"],
                ),
            )
            if relaxed_numeric_candidates:
                direct_numeric_candidate = relaxed_numeric_candidates[0]
                if direct_numeric_candidate["best_distance"] <= max(10, int((field.get("qcardinal") or 0) * 0.3)):
                    values = direct_numeric_candidate["values"]
                    notes = (
                        "A packed numeric vector without a matching dictionary stream was assigned directly. "
                        f"Source offset {direct_numeric_candidate['offset']}."
                    )
                    status = "exact" if direct_numeric_candidate["best_distance"] == 0 else "heuristic"
                    set_column(
                        table_state,
                        field,
                        values,
                        status,
                        notes,
                        [direct_numeric_candidate["offset"]],
                        "direct_numeric_vector",
                    )
                    continue

        candidates: list[tuple[int, dict[str, Any], dict[str, Any], str]] = []
        for dictionary_stream in dictionary_streams:
            for vector in table_vectors:
                if vector["offset"] in used_offsets:
                    continue
                score, scheme = score_relaxed_vector_assignment(field, dictionary_stream, vector, known_table_names=known_table_names)
                if score > -1_000_000 and scheme:
                    candidates.append((score, dictionary_stream, vector, scheme))

        if not candidates:
            continue

        candidates.sort(key=lambda item: (-item[0], item[2]["offset"], item[1]["offset"]))
        score, dictionary_stream, vector, scheme = candidates[0]
        if table_state["confidence"][field_name]["status"] != "missing":
            continue

        decoded_values = decode_indexed_values(vector, dictionary_stream["values"], scheme)
        if not any(value is not None for value in decoded_values):
            continue

        status = "exact" if vector["best_distance"] == 0 and scheme in {"zero_based", "one_based"} else "heuristic"
        notes = (
            f"Relaxed dictionary-index reconstruction using {vector['bits_per_value']}-bit values with {vector['header_bytes']} header bytes. "
            f"Dictionary stream at offset {dictionary_stream['offset']} and index vector at offset {vector['offset']}."
        )
        if scheme == "ranked_unique":
            notes += " The decoded values use ranked unique codes because the stream appears to contain a subset of the dictionary."
        elif scheme == "zero_null_one_based_fill_zero":
            notes += " Metadata shows the field is populated on every row, so index value 0 is mapped to the first dictionary value instead of null."
        if is_key_field(field):
            notes += " The field is key-like, so text dictionaries are allowed even when the source metadata marks it numeric."

        set_column(
            table_state,
            field,
            decoded_values,
            status,
            notes,
            [dictionary_stream["offset"], vector["offset"]],
            "relaxed_dictionary_index_vector",
        )
        table_state["mapping"][field_name].append(
            {
                "method": "relaxed_dictionary_index_vector",
                "dictionary_offset": dictionary_stream["offset"],
                "vector_offset": vector["offset"],
                "bits_per_value": vector["bits_per_value"],
                "header_bytes": vector["header_bytes"],
                "scheme": scheme,
                "score": score,
            }
        )
        used_offsets.add(vector["offset"])
        used_offsets.add(dictionary_stream["offset"])


def candidate_index_mapping_schemes(vector: dict[str, Any], dictionary_count: int, prefer_no_nulls: bool = False) -> list[str]:
    unique_values = set(vector["unique_values"])
    min_value = vector["min"]
    max_value = vector["max"]
    slack = max(2, int(dictionary_count * 0.08))
    schemes: list[str] = []

    if min_value >= 0 and max_value <= dictionary_count - 1:
        schemes.append("zero_based")
    if min_value >= 1 and max_value <= dictionary_count:
        schemes.append("one_based")
    if 0 in unique_values and max_value <= dictionary_count:
        non_zero = {value for value in unique_values if value != 0}
        if non_zero and min(non_zero) >= 1:
            schemes.append("zero_null_one_based")
            if prefer_no_nulls:
                schemes.append("zero_null_one_based_fill_zero")
    if prefer_no_nulls and 0 in unique_values and max_value <= dictionary_count + 1:
        non_zero = {value for value in unique_values if value != 0}
        if non_zero and min(non_zero) >= 1:
            schemes.append("zero_null_one_based_fill_zero")
    if 0 in unique_values and 1 not in unique_values and max_value <= dictionary_count + 1:
        non_reserved = {value for value in unique_values if value not in {0, 1}}
        if not non_reserved or min(non_reserved) >= 2:
            schemes.append("zero_null_skip_one")
    if len(unique_values) <= dictionary_count + slack:
        schemes.append("ranked_unique")
    if 0 in unique_values and len(unique_values) - 1 <= dictionary_count + slack:
        schemes.append("ranked_unique_null_zero")
    return list(dict.fromkeys(schemes))


def infer_index_mapping_scheme(vector: dict[str, Any], dictionary_count: int, prefer_no_nulls: bool = False) -> str | None:
    schemes = candidate_index_mapping_schemes(vector, dictionary_count, prefer_no_nulls=prefer_no_nulls)
    return schemes[0] if schemes else None


def scheme_quality_score(
    field: dict[str, Any],
    vector: dict[str, Any],
    dictionary_values: list[Any],
    scheme: str,
) -> int:
    decoded = decode_indexed_values(vector, dictionary_values, scheme)
    non_null = [value for value in decoded if value is not None]
    non_null_count = len(non_null)
    decoded_unique_count = len(set(non_null))
    row_count = vector["row_count"]
    field_total_count = int(field.get("qtotal_count") or 0)
    field_cardinality = int(field.get("qcardinal") or 0)
    prefer_no_nulls = field_total_count == row_count and row_count > 0

    score = 0
    if field_total_count > 0:
        score -= abs(non_null_count - field_total_count) * (3 if field_total_count <= 2000 else 1)
        if non_null_count == field_total_count:
            score += 140
        elif non_null_count >= field_total_count * 0.98:
            score += 60
    else:
        score += int((non_null_count / max(1, row_count)) * 40)

    if field_cardinality > 0:
        score -= abs(decoded_unique_count - field_cardinality) * 12
        if decoded_unique_count == field_cardinality:
            score += 140
        elif abs(decoded_unique_count - field_cardinality) <= 1:
            score += 50

    if scheme in {"zero_based", "one_based"}:
        score += 25
    elif scheme in {"zero_null_one_based", "zero_null_skip_one"}:
        score += 10
    elif scheme == "zero_null_one_based_fill_zero":
        score += 20 if prefer_no_nulls else -20
    elif scheme == "ranked_unique":
        score -= 10
    elif scheme == "ranked_unique_null_zero":
        score -= 25

    if prefer_no_nulls and non_null_count == row_count:
        score += 90
    if not prefer_no_nulls and "fill_zero" in scheme:
        score -= 40

    return score


def decode_indexed_values(vector: dict[str, Any], dictionary_values: list[Any], scheme: str) -> list[Any]:
    if scheme == "ranked_unique":
        ordered_codes = sorted(set(vector["values"]))
        mapping = {code: dictionary_values[index] for index, code in enumerate(ordered_codes[: len(dictionary_values)])}
        return [mapping.get(value) for value in vector["values"]]
    if scheme == "ranked_unique_null_zero":
        ordered_codes = sorted(code for code in set(vector["values"]) if code != 0)
        mapping = {0: None}
        mapping.update({code: dictionary_values[index] for index, code in enumerate(ordered_codes[: len(dictionary_values)])})
        return [mapping.get(value) for value in vector["values"]]

    decoded: list[Any] = []
    for value in vector["values"]:
        if scheme == "zero_based":
            decoded.append(dictionary_values[value] if 0 <= value < len(dictionary_values) else None)
        elif scheme == "one_based":
            decoded.append(dictionary_values[value - 1] if 1 <= value <= len(dictionary_values) else None)
        elif scheme == "zero_null_one_based":
            decoded.append(None if value == 0 else dictionary_values[value - 1] if 1 <= value <= len(dictionary_values) else None)
        elif scheme == "zero_null_one_based_fill_zero":
            decoded.append(dictionary_values[0] if value == 0 and dictionary_values else dictionary_values[value - 1] if 1 <= value <= len(dictionary_values) else None)
        elif scheme == "zero_null_skip_one":
            decoded.append(None if value in {0, 1} else dictionary_values[value - 2] if 2 <= value <= len(dictionary_values) + 1 else None)
        else:
            decoded.append(None)
    return decoded


def score_index_vector_assignment(
    field: dict[str, Any],
    dictionary_stream: dict[str, Any],
    vector: dict[str, Any],
    known_table_names: set[str] | None = None,
) -> tuple[int, str | None]:
    kind = field_value_kind(field)
    if kind == "numeric" and dictionary_stream["value_type"] not in {"double", "int"}:
        return -1_000_000, None
    if kind == "text" and dictionary_stream["value_type"] != "str":
        return -1_000_000, None

    dictionary_count = dictionary_stream["count"]
    field_cardinality = int(field.get("qcardinal") or 0)
    if field_cardinality and dictionary_count != field_cardinality:
        return -1_000_000, None

    prefer_no_nulls = int(field.get("qtotal_count") or 0) == vector["row_count"] and vector["row_count"] > 0
    schemes = candidate_index_mapping_schemes(vector, dictionary_count, prefer_no_nulls=prefer_no_nulls)
    if not schemes:
        return -1_000_000, None
    best_scheme = None
    best_scheme_score = -1_000_000
    for scheme in schemes:
        quality_score = scheme_quality_score(field, vector, dictionary_stream["values"], scheme)
        if quality_score > best_scheme_score:
            best_scheme = scheme
            best_scheme_score = quality_score
    scheme = best_scheme
    if not scheme:
        return -1_000_000, None

    dictionary_distance = min(
        abs(vector["unique_count"] - dictionary_count),
        abs((vector["unique_count"] - 1) - dictionary_count),
        abs(vector["max"] - dictionary_count),
        abs((vector["max"] - 1) - dictionary_count),
        abs((vector["max"] + 1) - dictionary_count),
    )
    max_distance = max(1, int(dictionary_count * (0.35 if is_key_field(field) else 0.15)))
    if dictionary_distance > max_distance:
        return -1_000_000, None

    score = 200
    score -= dictionary_distance * (2 if is_key_field(field) else 8)
    if vector["best_cardinality"] == dictionary_count:
        score += 60
    if vector["best_distance"] == 0:
        score += 30
    if vector["unique_count"] == dictionary_count:
        score += 40
    elif vector["unique_count"] == dictionary_count + 1:
        score += 30
    if vector["max"] == dictionary_count - 1:
        score += 25
    elif vector["max"] == dictionary_count:
        score += 15
    elif vector["max"] == dictionary_count + 1:
        score += 10

    field_name = field["qname"].lower()
    if kind == "text" and "code" in field_name:
        score += 10
    if kind == "numeric" and dictionary_stream["value_type"] in {"double", "int"} and any(token in field_name for token in ["amount", "price", "fundsize", "%", "year"]):
        score += 10
    if vector["bits_per_value"] in {8, 16}:
        score += 10
    if kind == "text":
        score += string_semantic_bonus(field, dictionary_stream, known_table_names=known_table_names)
    else:
        score += numeric_semantic_bonus(field, dictionary_stream)
    score += best_scheme_score

    return score, scheme


def apply_index_vector_reconstruction(
    spec: dict[str, Any],
    table_state: dict[str, Any],
    scalar_streams: list[dict[str, Any]],
    index_vectors: list[dict[str, Any]],
    used_offsets: set[int],
    known_table_names: set[str] | None = None,
) -> None:
    table_vectors = [vector for vector in index_vectors if vector["table"] == spec["name"]]
    candidates: list[tuple[int, dict[str, Any], dict[str, Any], dict[str, Any], str]] = []

    for field in spec["fields"]:
        field_name = field["qname"]
        if table_state["confidence"][field_name]["status"] != "missing":
            continue

        dictionary_streams = [
            stream
            for stream in scalar_streams
            if stream["count"] == int(field.get("qcardinal") or 0)
            and scalar_stream_matches_field(field, stream)
        ]
        add_candidate_offsets(table_state, field_name, [stream["offset"] for stream in dictionary_streams])

        for dictionary_stream in dictionary_streams:
            for vector in table_vectors:
                if vector["offset"] in used_offsets:
                    continue
                score, scheme = score_index_vector_assignment(field, dictionary_stream, vector, known_table_names=known_table_names)
                if score > 0 and scheme:
                    candidates.append((score, field, dictionary_stream, vector, scheme))

    used_vector_offsets: set[int] = set()
    used_dictionary_offsets: set[int] = set()
    candidates.sort(key=lambda item: (-item[0], item[3]["offset"], item[2]["offset"], item[1]["qname"]))

    for score, field, dictionary_stream, vector, scheme in candidates:
        field_name = field["qname"]
        if table_state["confidence"][field_name]["status"] != "missing":
            continue
        if vector["offset"] in used_vector_offsets or dictionary_stream["offset"] in used_dictionary_offsets:
            continue

        decoded_values = decode_indexed_values(vector, dictionary_stream["values"], scheme)
        coverage = sum(value is not None for value in decoded_values)
        if coverage == 0:
            continue

        status = "exact" if vector["best_distance"] == 0 and scheme in {"zero_based", "one_based"} else "heuristic"
        notes = (
            f"Dictionary-index reconstruction using {vector['bits_per_value']}-bit values with {vector['header_bytes']} header bytes. "
            f"Dictionary stream at offset {dictionary_stream['offset']} and index vector at offset {vector['offset']}."
        )
        if scheme == "zero_null_skip_one":
            notes += " Index value 0 is treated as null and 1 is reserved/unused."
        elif scheme == "zero_null_one_based":
            notes += " Index value 0 is treated as null."
        elif scheme == "zero_null_one_based_fill_zero":
            notes += " Metadata shows the field is populated on every row, so index value 0 is mapped to the first dictionary value instead of null."

        set_column(
            table_state,
            field,
            decoded_values,
            status,
            notes,
            [dictionary_stream["offset"], vector["offset"]],
            "dictionary_index_vector",
        )
        table_state["mapping"][field_name].append(
            {
                "method": "dictionary_index_vector",
                "dictionary_offset": dictionary_stream["offset"],
                "vector_offset": vector["offset"],
                "bits_per_value": vector["bits_per_value"],
                "header_bytes": vector["header_bytes"],
                "scheme": scheme,
                "score": score,
            }
        )
        used_vector_offsets.add(vector["offset"])
        used_dictionary_offsets.add(dictionary_stream["offset"])
        used_offsets.add(vector["offset"])


def score_row_stream(field: dict[str, Any], stream: dict[str, Any], row_count: int) -> int:
    if stream["count"] != row_count:
        return -1_000_000
    if field_value_kind(field) == "numeric" and stream["value_type"] not in {"double", "int"}:
        return -1_000_000
    if field_value_kind(field) == "text" and stream["value_type"] != "str":
        return -1_000_000

    field_name = field["qname"].lower()
    score = 100
    traits = stream["traits"]
    field_cardinality = int(field.get("qcardinal") or 0)
    field_total_count = int(field.get("qtotal_count") or 0)
    unique_count = int(traits.get("unique_count") or 0)

    if field_cardinality > 0:
        if unique_count == field_cardinality:
            score += 60
        elif unique_count == row_count and field_cardinality != row_count:
            score -= 80
        else:
            score -= min(abs(unique_count - field_cardinality), row_count) // 10

    if "$key" in (field.get("qtags") or []) and field_total_count == 0 and 0 < field_cardinality < row_count:
        score -= 120

    if field_value_kind(field) == "numeric":
        if "id" in field_name and traits.get("all_integral"):
            score += 40
            if traits.get("is_monotonic_non_decreasing"):
                score += 20
            if unique_count == row_count:
                score += 30
        if "date" in field_name and 30_000 <= traits.get("min", 0) <= 60_000 and traits.get("max", 0) <= 60_000:
            score += 35
        if "amount" in field_name and traits.get("average_abs", 0) > 1000:
            score += 12
        if "price" in field_name and 0 <= traits.get("average_abs", 0) < 10_000:
            score += 12
        if "quantity" in field_name and traits.get("all_integral"):
            score += 15
        if "$integer" in (field.get("qtags") or []) and traits.get("all_integral"):
            score += 10
        if "cusip" in field_name:
            score -= 200
        if "quantity" in field_name and unique_count == row_count and field_cardinality != row_count:
            score -= 180
        if ("price" in field_name or "amount" in field_name or field_name == "value") and unique_count == row_count and field_cardinality != row_count:
            score -= 120
        if ("date" in field_name or "period" in field_name) and not looks_like_date_serials(stream):
            score -= 180
        score += numeric_semantic_bonus(field, stream)
    else:
        average_length = traits.get("average_length", 0)
        if "currency" in field_name and traits.get("all_upper_alpha"):
            score += 20
        if "symbol" in field_name and traits.get("all_currency_symbols"):
            score += 30
        if "name" in field_name and average_length > 10:
            score += 12
        if "link" in field_name and traits.get("all_hyphenated"):
            score += 20
        if "cusip" in field_name and all(re.fullmatch(r"[A-Z0-9]{6,}", value or "") for value in stream["values"][: min(10, len(stream["values"]))]):
            score += 18
        if "status" in field_name and average_length < 16:
            score += 8
        score += string_semantic_bonus(field, stream)
    return score


def apply_general_rowwise_reconstruction(
    spec: dict[str, Any],
    table_state: dict[str, Any],
    scalar_streams: list[dict[str, Any]],
    used_offsets: set[int],
    known_table_names: set[str] | None = None,
) -> None:
    candidate_pairs: list[tuple[int, dict[str, Any], dict[str, Any]]] = []

    for field in spec["fields"]:
        field_name = field["qname"]
        if table_state["confidence"][field_name]["status"] != "missing":
            continue

        candidate_offsets = [
            stream["offset"]
            for stream in scalar_streams
            if stream["count"] == field.get("qcardinal")
            and (
                (field_value_kind(field) == "numeric" and stream["value_type"] in {"double", "int"})
                or (field_value_kind(field) == "text" and stream["value_type"] == "str")
            )
        ]
        add_candidate_offsets(table_state, field_name, candidate_offsets)

        for stream in scalar_streams:
            if stream["offset"] in used_offsets:
                continue
            score = score_row_stream(field, stream, spec["row_count"])
            if score > 0:
                candidate_pairs.append((score, field, stream))

    assigned_fields: set[str] = set()
    assigned_offsets: set[int] = set()
    candidate_pairs.sort(key=lambda item: (-item[0], item[2]["offset"], item[1]["qname"]))

    for score, field, stream in candidate_pairs:
        field_name = field["qname"]
        if field_name in assigned_fields or stream["offset"] in assigned_offsets or stream["offset"] in used_offsets:
            continue

        status = "heuristic"
        notes = "A row-count-matching scalar stream was selected using deterministic field/type heuristics."
        if field_name.lower().endswith("id") and stream["traits"].get("all_integral"):
            notes = "A row-count-matching integral stream with identifier-like uniqueness was selected for this identifier column."
        elif "date" in field_name.lower():
            notes = "A row-count-matching numeric stream with date-like values was selected for this date column."

        set_column(
            table_state,
            field,
            stream["values"],
            status,
            notes,
            [stream["offset"]],
            "rowwise_scalar_stream",
        )
        assigned_fields.add(field_name)
        assigned_offsets.add(stream["offset"])
        used_offsets.add(stream["offset"])


def compute_table_status(confidence: dict[str, dict[str, Any]]) -> str:
    if not confidence:
        return "exact"
    statuses = {item["status"] for item in confidence.values()} if confidence else {"missing"}
    if statuses == {"exact"}:
        return "exact"
    if statuses == {"heuristic"}:
        return "heuristic"
    if "partial" in statuses or ("missing" in statuses and len(statuses) > 1):
        return "partial"
    if "heuristic" in statuses and len(statuses) > 1:
        return "partial"
    if statuses == {"missing"}:
        return "missing"
    return "partial"


def build_manifest_rows(
    specs: list[dict[str, Any]],
    confidence_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    manifest_rows: list[dict[str, Any]] = []
    for spec in specs:
        confidence = confidence_payload[spec["name"]]
        filled_columns = sum(item["status"] != "missing" for item in confidence.values())
        source_offsets = sorted(
            {
                offset
                for item in confidence.values()
                for offset in item.get("source_block_offsets", [])
            }
        )
        manifest_rows.append(
            {
                "table": spec["name"],
                "expected_row_count": spec["row_count"],
                "exported_row_count": spec["row_count"] if spec["fields"] else 0,
                "total_columns": len(spec["fields"]),
                "exported_columns": filled_columns,
                "status": compute_table_status(confidence),
                "source_block_offsets": source_offsets,
                "files": {
                    "parquet": f"tables/{spec['name']}.parquet",
                    "tsv": f"tables/{spec['name']}.tsv",
                },
            }
        )
    return manifest_rows


def build_exact_currency_lookup(
    table_payload: dict[str, Any],
    confidence_payload: dict[str, Any],
) -> tuple[dict[str, str | None], list[int]]:
    currency_payload = table_payload.get("Currency")
    currency_confidence = confidence_payload.get("Currency", {})
    if not currency_payload or "_CURRENCY" not in currency_payload["columns"] or "_SYMBOL" not in currency_payload["columns"]:
        return {}, []
    if currency_confidence.get("_CURRENCY", {}).get("status") != "exact":
        return {}, []
    if currency_confidence.get("_SYMBOL", {}).get("status") != "exact":
        return {}, []

    codes = currency_payload["columns"]["_CURRENCY"]
    symbols = currency_payload["columns"]["_SYMBOL"]
    lookup = {
        code: symbol
        for code, symbol in zip(codes, symbols)
        if code not in {None, ""}
    }
    source_offsets = sorted(
        {
            *currency_confidence.get("_CURRENCY", {}).get("source_block_offsets", []),
            *currency_confidence.get("_SYMBOL", {}).get("source_block_offsets", []),
        }
    )
    return lookup, source_offsets


def corresponding_currency_field_name(symbol_field_name: str, available_fields: set[str]) -> str | None:
    if "symbol" not in symbol_field_name.lower():
        return None
    direct_candidate = re.sub(r"symbol", "Currency", symbol_field_name, count=1, flags=re.IGNORECASE)
    if direct_candidate in available_fields:
        return direct_candidate
    lowered = {name.lower(): name for name in available_fields}
    lowered_candidate = symbol_field_name.lower().replace("symbol", "currency", 1)
    return lowered.get(lowered_candidate)


def apply_currency_symbol_postprocessing(
    table_payload: dict[str, Any],
    confidence_payload: dict[str, Any],
    mapping_payload: dict[str, Any],
) -> None:
    currency_lookup, lookup_offsets = build_exact_currency_lookup(table_payload, confidence_payload)
    if not currency_lookup:
        return

    for table_name, payload in table_payload.items():
        available_fields = {field["qname"] for field in payload["fields"]}
        for field in payload["fields"]:
            field_name = field["qname"]
            if "symbol" not in field_name.lower():
                continue

            currency_field_name = corresponding_currency_field_name(field_name, available_fields)
            if not currency_field_name:
                continue

            currency_values = payload["columns"].get(currency_field_name)
            if currency_values is None:
                continue

            current_values = payload["columns"].get(field_name, [])
            known_pairs = [
                (code, symbol)
                for code, symbol in zip(currency_values, current_values)
                if code in currency_lookup and symbol not in {None, ""}
            ]
            if not known_pairs:
                continue

            mismatches = sum(symbol != currency_lookup.get(code) for code, symbol in known_pairs)
            by_currency: dict[str, set[str]] = {}
            for code, symbol in known_pairs:
                by_currency.setdefault(str(code), set()).add(str(symbol))
            inconsistent = any(len(symbols) > 1 for symbols in by_currency.values())
            if mismatches == 0 and not inconsistent:
                continue

            derived_values = [currency_lookup.get(code) or STANDARD_CURRENCY_SYMBOLS.get(str(code)) for code in currency_values]
            payload["columns"][field_name] = derived_values

            source_offsets = sorted(
                {
                    *lookup_offsets,
                    *confidence_payload[table_name].get(currency_field_name, {}).get("source_block_offsets", []),
                }
            )
            confidence_payload[table_name][field_name] = {
                "status": "heuristic",
                "coverage_ratio": 1.0,
                "inferred_type": "string",
                "notes": (
                    f"Derived from the exact Currency lookup table using '{currency_field_name}' because the directly decoded symbol stream did not map consistently to known currency codes. "
                    "Currencies absent from the lookup fall back to a small built-in ISO symbol map when available; otherwise they are exported as null."
                ),
                "source_block_offsets": source_offsets,
                "candidate_dictionary_offsets": confidence_payload[table_name][field_name].get("candidate_dictionary_offsets", []),
            }
            mapping_payload[table_name][field_name].append(
                {
                    "method": "currency_lookup_derivation",
                    "currency_field": currency_field_name,
                    "lookup_table": "Currency",
                    "lookup_source_block_offsets": lookup_offsets,
                    "status": "heuristic",
                }
            )


def parse_temporal_format_hints(script: str) -> dict[str, str]:
    hints: dict[str, str] = {}
    pattern = re.compile(
        r"Date\s*\(.*?,\s*'([^']+)'\s*\)\s+as\s+\"([^\"]+)\"",
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(script):
        qlik_format, field_name = match.groups()
        hints[field_name] = qlik_format
    return hints


def qlik_datetime_from_serial(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    return QLIK_EPOCH + timedelta(days=float(value))


def qlik_format_to_strftime(qlik_format: str) -> str:
    mapping = {
        "YYYY": "%Y",
        "MMM": "%b",
        "MM": "%m",
        "DD": "%d",
        "hh": "%H",
        "mm": "%M",
        "ss": "%S",
    }
    result = qlik_format
    for token in sorted(mapping, key=len, reverse=True):
        result = result.replace(token, mapping[token])
    return result


def format_temporal_value(field: dict[str, Any], value: Any, temporal_hints: dict[str, str]) -> Any:
    if value in {None, ""} or field_value_kind(field) != "numeric":
        return value

    tags = set(field.get("qtags") or [])
    if "$date" not in tags and "$timestamp" not in tags:
        return value

    dt = qlik_datetime_from_serial(value)
    if dt is None:
        return value

    explicit_format = temporal_hints.get(field["qname"])
    if explicit_format:
        return dt.strftime(qlik_format_to_strftime(explicit_format))
    if "$timestamp" in tags and not (is_integer_field(field) and float(value).is_integer()):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.date().isoformat()


def finalize_table_exports(
    specs: list[dict[str, Any]],
    scalar_streams: list[dict[str, Any]],
    index_vectors: list[dict[str, Any]],
    script: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    tables_dir_payload: dict[str, Any] = {}
    confidence_payload: dict[str, Any] = {}
    mapping_payload: dict[str, Any] = {}
    known_table_names = {spec["name"] for spec in specs}

    for spec in specs:
        table_state = create_empty_table_state(spec)
        used_offsets = apply_small_table_reconstruction(spec, table_state, scalar_streams, script)
        apply_index_vector_reconstruction(spec, table_state, scalar_streams, index_vectors, used_offsets, known_table_names=known_table_names)
        apply_constant_field_reconstruction(spec, table_state, scalar_streams, used_offsets)
        apply_relaxed_field_reconstruction(spec, table_state, scalar_streams, index_vectors, used_offsets, known_table_names=known_table_names)
        apply_general_rowwise_reconstruction(spec, table_state, scalar_streams, used_offsets, known_table_names=known_table_names)

        tables_dir_payload[spec["name"]] = {
            "columns": table_state["columns"],
            "row_count": spec["row_count"],
            "fields": spec["fields"],
        }
        confidence_payload[spec["name"]] = table_state["confidence"]
        mapping_payload[spec["name"]] = table_state["mapping"]
    apply_currency_symbol_postprocessing(tables_dir_payload, confidence_payload, mapping_payload)
    manifest_rows = build_manifest_rows(specs, confidence_payload)
    return tables_dir_payload, manifest_rows, confidence_payload, mapping_payload


def write_tsv(path: Path, fields: list[dict[str, Any]], columns: dict[str, list[Any]], row_count: int, temporal_hints: dict[str, str]) -> None:
    field_order = [field["qname"] for field in fields]
    fields_by_name = {field["qname"]: field for field in fields}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        if field_order:
            writer.writerow(field_order)
        for row_index in range(row_count):
            row = []
            for field_name in field_order:
                value = columns[field_name][row_index]
                formatted = format_temporal_value(fields_by_name[field_name], value, temporal_hints)
                row.append("" if formatted is None else formatted)
            if field_order:
                writer.writerow(row)


def arrow_array_for_field(field: dict[str, Any], values: list[Any]) -> Any:
    if pa is None:
        raise RuntimeError("pyarrow is required for Parquet export. Install requirements.txt first.")

    if field_value_kind(field) != "numeric":
        return pa.array(values, type=pa.string())

    if is_integer_field(field):
        coerced = [None if value is None else int(value) for value in values]
        return pa.array(coerced, type=pa.int64())

    coerced = [None if value is None else float(value) for value in values]
    return pa.array(coerced, type=pa.float64())


def write_parquet(path: Path, fields: list[dict[str, Any]], columns: dict[str, list[Any]]) -> None:
    if pa is None or pq is None:
        raise RuntimeError("pyarrow is required for Parquet export. Install requirements.txt first.")
    arrays = []
    names = []
    for field in fields:
        names.append(field["qname"])
        arrays.append(arrow_array_for_field(field, columns[field["qname"]]))
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, path)


def write_table_outputs(output_dir: Path, table_payload: dict[str, Any], temporal_hints: dict[str, str]) -> None:
    tables_dir = output_dir / "tables"
    ensure_dir(tables_dir)
    for table_name, payload in table_payload.items():
        field_order = [field["qname"] for field in payload["fields"]]
        write_tsv(
            tables_dir / f"{table_name}.tsv",
            payload["fields"],
            payload["columns"],
            payload["row_count"] if field_order else 0,
            temporal_hints,
        )
        write_parquet(tables_dir / f"{table_name}.parquet", payload["fields"], payload["columns"])


def write_summary(
    path: Path,
    manifest: dict[str, Any],
    summary: dict[str, Any],
    assets: list[dict[str, Any]],
    data_sources: dict[str, list[str]],
    table_manifest: list[dict[str, Any]],
) -> None:
    status_counts = Counter(row["status"] for row in table_manifest)
    tables_enabled = bool(table_manifest) or manifest.get("tables_enabled", False)
    lines = [
        f"App: {summary['app'].get('qTitle') or manifest['source_filename']}",
        f"Source file: {manifest['source_filename']}",
        f"SHA-256: {manifest['sha256']}",
        f"QVF size: {manifest['file_size_bytes']} bytes",
        f"Decoded blocks: {manifest['decoded_block_count']}",
        f"Decoded JSON objects: {manifest['decoded_object_count']}",
        f"Sheets: {len(summary['sheets'])}",
        f"Measures: {len(summary['measures'])}",
        f"Dimensions: {len(summary['dimensions'])}",
        f"Variables: {len(summary['variables'])}",
        f"Master objects: {len(summary['masterobjects'])}",
        f"Extracted assets: {len(assets)}",
        f"Exported tables: {len(table_manifest)}" if tables_enabled else "Exported tables: skipped",
        f"Exact tables: {status_counts.get('exact', 0)}" if tables_enabled else "",
        f"Partial tables: {status_counts.get('partial', 0)}" if tables_enabled else "",
        f"Heuristic tables: {status_counts.get('heuristic', 0)}" if tables_enabled else "",
        "",
        "Sheets:",
    ]
    lines = [line for line in lines if line]
    lines.extend(f"- {sheet['title']} ({sheet['id']})" for sheet in summary["sheets"])
    lines.append("")
    if tables_enabled:
        lines.append("Table export summary:")
        lines.extend(
            f"- {row['table']}: {row['status']} ({row['exported_columns']}/{row['total_columns']} columns, {row['exported_row_count']}/{row['expected_row_count']} rows)"
            for row in table_manifest
        )
        lines.append("")
    lines.append("")
    lines.append("Data sources:")
    lines.extend(f"- {item}" for item in data_sources["lib_references"])
    lines.extend(f"- {item}" for item in data_sources["file_references"] if item not in data_sources["lib_references"])
    lines.append("")
    lines.append("Notes:")
    lines.append("- This output is produced by direct file analysis of the QVF structure.")
    if tables_enabled:
        lines.append("- Table exports are confidence-marked and may contain null padding where row alignment remains unresolved.")
    else:
        lines.append("- Table reconstruction was skipped for this run.")
    lines.append("- Opaque blocks remain listed under raw/unknown-blocks.json for follow-up parser work.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def zip_directory(source_dir: Path, target_zip: Path) -> None:
    if target_zip.exists():
        target_zip.unlink()
    ensure_dir(target_zip.parent)
    with ZipFile(target_zip, "w", compression=ZIP_DEFLATED) as archive:
        for file_path in sorted(source_dir.rglob("*")):
            if file_path == target_zip or file_path.is_dir():
                continue
            archive.write(file_path, file_path.relative_to(source_dir))


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file).resolve()
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1
    if input_path.suffix.lower() != ".qvf":
        print("Only .qvf files are supported.", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir).resolve()
    raw_dir = output_dir / args.raw_dir_name
    assets_dir = output_dir / "assets"
    ensure_dir(output_dir)
    ensure_dir(raw_dir)
    ensure_dir(assets_dir)

    blob = input_path.read_bytes()
    format_blocks, decoded_objects = decode_format_blocks(blob)
    summary = summarise_objects(decoded_objects)
    strings = extract_strings(blob)
    assets, binary_unknowns = extract_binary_assets(blob, format_blocks, assets_dir)
    scalar_streams = discover_scalar_streams(blob, format_blocks)
    data_sources = collect_data_sources(summary["script"], strings)
    block_rows, opaque_rows = build_block_index(format_blocks, blob)
    opaque_rows.extend(binary_unknowns)
    table_specs = build_table_specs(summary["data_model_metadata"])
    non_scalar_streams = classify_non_scalar_streams(blob, format_blocks, table_specs)
    index_vectors = discover_index_vectors(blob, format_blocks, table_specs)

    temporal_hints = parse_temporal_format_hints(summary["script"])

    if args.skip_tables:
        table_payload = {}
        table_manifest = []
        table_confidence = {}
        table_mapping = {}
    else:
        table_payload, table_manifest, table_confidence, table_mapping = finalize_table_exports(
            table_specs,
            scalar_streams,
            index_vectors,
            summary["script"],
        )

    manifest = {
        "source_filename": input_path.name,
        "source_path": str(input_path),
        "file_size_bytes": len(blob),
        "sha256": sha256_bytes(blob),
        "decoded_block_count": len(format_blocks),
        "decoded_object_count": len(decoded_objects),
        "scalar_stream_count": len(scalar_streams),
        "non_scalar_stream_count": len(non_scalar_streams),
        "index_vector_count": len(index_vectors),
        "block_type_counts": dict(
            sorted(
                Counter(
                    block.get("classification") or block.get("format") or "unknown"
                    for block in format_blocks
                ).items()
            )
        ),
        "app_title": summary["app"].get("qTitle"),
        "product_version": summary["app"].get("qSavedInProductVersion"),
        "tables_enabled": not args.skip_tables,
        "table_count": len(table_manifest),
        "table_status_counts": dict(sorted(Counter(row["status"] for row in table_manifest).items())),
        "generated_files": [],
    }

    app_json = {
        **summary["app"],
        "app_properties": summary["app_properties"],
        "load_model_summary": summary["load_model"].get("qRoot", {}).get("qProperty", {}),
    }

    json_dump(output_dir / "manifest.json", manifest)
    json_dump(output_dir / "app.json", app_json)
    json_dump(output_dir / "sheets.json", summary["sheets"])
    json_dump(output_dir / "masterobjects.json", summary["masterobjects"])
    json_dump(output_dir / "measures.json", summary["measures"])
    json_dump(output_dir / "dimensions.json", summary["dimensions"])
    json_dump(output_dir / "variables.json", summary["variables"])
    json_dump(output_dir / "data-sources.json", data_sources)
    json_dump(output_dir / "assets.json", assets)
    json_dump(output_dir / "data-model.json", summary["data_model_metadata"])
    json_dump(output_dir / "load-model.json", summary["load_model"])
    json_dump(output_dir / "color-maps.json", summary["color_maps"])
    (output_dir / "script.qvs").write_text(summary["script"], encoding="utf-8")
    write_summary(output_dir / "summary.txt", manifest, summary, assets, data_sources, table_manifest)
    jsonl_dump(raw_dir / "blocks.jsonl", block_rows)
    jsonl_dump(raw_dir / "decoded-objects.jsonl", decoded_objects)
    json_dump(raw_dir / "unknown-blocks.json", opaque_rows)
    json_dump(raw_dir / "non-scalar-streams.json", non_scalar_streams)
    json_dump(raw_dir / "index-vectors.json", [
        {
            "offset": vector["offset"],
            "table": vector["table"],
            "row_count": vector["row_count"],
            "bits_per_value": vector["bits_per_value"],
            "header_bytes": vector["header_bytes"],
            "min": vector["min"],
            "max": vector["max"],
            "unique_count": vector["unique_count"],
            "best_cardinality": vector["best_cardinality"],
            "best_distance": vector["best_distance"],
        }
        for vector in index_vectors
    ])
    json_dump(raw_dir / "scalar-streams.json", [
        {
            "offset": stream["offset"],
            "count": stream["count"],
            "value_type": stream["value_type"],
            "traits": stream["traits"],
            "head": stream["values"][:10],
            "tail": stream["values"][-10:],
        }
        for stream in scalar_streams
    ])
    if not args.skip_tables:
        write_table_outputs(output_dir, table_payload, temporal_hints)
        json_dump(output_dir / "tables" / "_manifest.json", table_manifest)
        json_dump(output_dir / "tables" / "_confidence.json", table_confidence)
        json_dump(raw_dir / "table-block-mapping.json", table_mapping)
    (raw_dir / "string-findings.txt").write_text("\n".join(strings) + "\n", encoding="utf-8")

    manifest["generated_files"] = sorted(
        str(path.relative_to(output_dir)) for path in output_dir.rglob("*") if path.is_file()
    )
    json_dump(output_dir / "manifest.json", manifest)

    if args.zip_path:
        zip_directory(output_dir, Path(args.zip_path).resolve())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
