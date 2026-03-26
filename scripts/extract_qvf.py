#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
import uuid
import zlib
from collections import Counter
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

FORMAT_MARKER_RE = re.compile(rb'\{"format":"(gzjson|binary)"\}\x00')
ASCII_STRING_RE = re.compile(rb'[\x20-\x7e]{6,}')
UTF16LE_STRING_RE = re.compile(rb'(?:[\x20-\x7e]\x00){6,}')
WINDOW_BYTES = 1400
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".bmp")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract metadata from a QVF file without a Qlik runtime."
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

    seen_offsets: set[int] = set()
    for offset in range(len(blob) - 1):
        if blob[offset] != 0x78 or blob[offset + 1] not in (0x01, 0x5E, 0x9C, 0xDA):
            continue
        if offset in seen_offsets:
            continue

        range_hit = any(start <= offset < end for start, end in known_ranges)
        try:
            stream = zlib.decompressobj()
            output = stream.decompress(blob[offset:])
            consumed = len(blob[offset:]) - len(stream.unused_data)
            if consumed <= 0:
                raise zlib.error("empty stream")
        except zlib.error as error:
            opaque_rows.append(
                {
                    "offset": offset,
                    "kind": "zlib_stream",
                    "reason": str(error),
                    "within_known_payload": range_hit,
                }
            )
            continue

        seen_offsets.add(offset)
        block_rows.append(
            {
                "block_type": "zlib_stream",
                "offset": offset,
                "compressed_size": consumed,
                "decompressed_size": len(output),
                "within_known_payload": range_hit,
                "preview": output[:120].decode("utf-8", errors="ignore"),
            }
        )

    return block_rows, opaque_rows


def write_summary(
    path: Path,
    manifest: dict[str, Any],
    summary: dict[str, Any],
    assets: list[dict[str, Any]],
    data_sources: dict[str, list[str]],
) -> None:
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
        "",
        "Sheets:",
    ]
    lines.extend(f"- {sheet['title']} ({sheet['id']})" for sheet in summary["sheets"])
    lines.append("")
    lines.append("Data sources:")
    lines.extend(f"- {item}" for item in data_sources["lib_references"])
    lines.extend(f"- {item}" for item in data_sources["file_references"] if item not in data_sources["lib_references"])
    lines.append("")
    lines.append("Notes:")
    lines.append("- This output is produced by direct file analysis of the QVF structure.")
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
    data_sources = collect_data_sources(summary["script"], strings)
    block_rows, opaque_rows = build_block_index(format_blocks, blob)
    opaque_rows.extend(binary_unknowns)

    manifest = {
        "source_filename": input_path.name,
        "source_path": str(input_path),
        "file_size_bytes": len(blob),
        "sha256": sha256_bytes(blob),
        "decoded_block_count": len(format_blocks),
        "decoded_object_count": len(decoded_objects),
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
    write_summary(output_dir / "summary.txt", manifest, summary, assets, data_sources)
    jsonl_dump(raw_dir / "blocks.jsonl", block_rows)
    jsonl_dump(raw_dir / "decoded-objects.jsonl", decoded_objects)
    json_dump(raw_dir / "unknown-blocks.json", opaque_rows)
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
