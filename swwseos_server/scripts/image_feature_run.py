import base64
import hashlib
import importlib.util
import json
import os
import re
import shutil
import struct
import sys
import tempfile
import urllib.parse

HAS_CV2 = importlib.util.find_spec("cv2") is not None
HAS_NUMPY = importlib.util.find_spec("numpy") is not None
HAS_PYTESSERACT = importlib.util.find_spec("pytesseract") is not None
HAS_TESSERACT_BIN = shutil.which("tesseract") is not None

if HAS_CV2:
    import cv2
else:
    cv2 = None

if HAS_NUMPY:
    import numpy as np
else:
    np = None

if HAS_PYTESSERACT:
    import pytesseract
else:
    pytesseract = None

DATA_URL_RE = re.compile(r"^data:(?P<mime>[^;,]+)?(?P<extra>(?:;[^,]+)*),(?P<data>.*)$", re.IGNORECASE)
HTTP_PREFIXES = ("http://", "https://")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
GIF_SIGNATURES = (b"GIF87a", b"GIF89a")


def out(obj):
    print(json.dumps(obj, ensure_ascii=False, default=lambda value: value.tolist() if hasattr(value, "tolist") else str(value)))


def ok(data=None):
    return {"ok": True, "data": data or {}}


def infer_extension(reference="", mime=""):
    ref = str(reference or "").strip().lower()
    parsed = urllib.parse.urlparse(ref)
    path = parsed.path or ref
    ext = os.path.splitext(path)[1].lstrip(".")
    if ext:
      return ext
    mime_value = str(mime or "").split(";")[0].strip().lower()
    mime_map = {
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/webp": "webp",
        "image/gif": "gif",
        "image/bmp": "bmp",
        "image/tiff": "tiff",
        "image/svg+xml": "svg",
    }
    return mime_map.get(mime_value, "")


def parse_png_size(data):
    if not data or len(data) < 24 or not data.startswith(PNG_SIGNATURE):
        return None
    width, height = struct.unpack(">II", data[16:24])
    return int(width), int(height)


def parse_gif_size(data):
    if not data or len(data) < 10 or not any(data.startswith(signature) for signature in GIF_SIGNATURES):
        return None
    width, height = struct.unpack("<HH", data[6:10])
    return int(width), int(height)


def extract_dimensions(data):
    return parse_png_size(data) or parse_gif_size(data)


def decode_data_url(reference):
    match = DATA_URL_RE.match(str(reference or "").strip())
    if not match:
        return None, "", ""
    mime = str(match.group("mime") or "").strip().lower()
    data = match.group("data") or ""
    extra = str(match.group("extra") or "").lower()
    raw = urllib.parse.unquote_to_bytes(data)
    if ";base64" in extra:
        raw = base64.b64decode(raw)
    return raw, infer_extension("", mime), mime


def normalize_local_path(reference, base_dir=""):
    raw = str(reference or "").strip()
    if raw.startswith("file://"):
        parsed = urllib.parse.urlparse(raw)
        raw = urllib.parse.unquote(parsed.path or "")
        if os.name == "nt" and raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
            raw = raw[1:]
    candidates = []
    if os.path.isabs(raw):
        candidates.append(raw)
    if base_dir:
        candidates.append(os.path.join(base_dir, raw.lstrip("/\\")))
    candidates.append(os.path.join(os.getcwd(), raw.lstrip("/\\")))
    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        normalized = os.path.normpath(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        if os.path.isfile(normalized):
            return normalized
    return ""


def load_reference_bytes(reference, base_dir=""):
    ref = str(reference or "").strip()
    if not ref:
        return None, "", "", "empty"
    if ref.lower().startswith("data:image/"):
        raw, ext, mime = decode_data_url(ref)
        return raw, ext, mime, "inline"
    if ref.lower().startswith(HTTP_PREFIXES):
        return None, infer_extension(ref, ""), "", "remote"
    local_path = normalize_local_path(ref, base_dir)
    if local_path:
        with open(local_path, "rb") as handle:
            return handle.read(), infer_extension(local_path, ""), "", "local"
    return None, infer_extension(ref, ""), "", "missing"


def hash_features(reference):
    digest = hashlib.sha1(str(reference or "").encode("utf-8")).digest()
    return [
        round(int.from_bytes(digest[idx:idx + 4], "big") / 0xFFFFFFFF, 6)
        for idx in range(0, 8, 4)
    ]


def decode_with_cv2(raw):
    if not (HAS_CV2 and HAS_NUMPY and raw):
        return None
    try:
        arr = np.frombuffer(raw, dtype=np.uint8)
        if arr.size == 0:
            return None
        image = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        return image
    except Exception:
        return None


def build_preview_row(reference, label, width, height, byte_size, runtime):
    return {
        "imageRef": reference,
        "label": label,
        "width": int(width or 0),
        "height": int(height or 0),
        "byteSize": int(byte_size or 0),
        "runtime": runtime,
    }


def capabilities():
    return ok({
        "cv2": HAS_CV2,
        "numpy": HAS_NUMPY,
        "pytesseract": HAS_PYTESSERACT,
        "tesseractBinary": HAS_TESSERACT_BIN,
        "supportedInputs": ["data_url", "local_path", "server_relative_path", "remote_url_preview"],
        "fallbackRuntime": True,
        "openCvFeatureRuntime": HAS_CV2 and HAS_NUMPY,
        "ocrDirectRuntime": HAS_PYTESSERACT and HAS_TESSERACT_BIN,
        "ocrFallbackRuntime": True,
    })


def normalize_ocr_text(value=""):
    return re.sub(r"\s+", " ", str(value or "").strip())


def extract_fallback_text(reference):
    ref = str(reference or "").strip()
    if not ref:
        return ""
    if ref.lower().startswith("data:image/"):
        return ""
    parsed = urllib.parse.urlparse(ref)
    base = os.path.basename(parsed.path or ref)
    stem = os.path.splitext(base)[0]
    cleaned = re.sub(r"[_\-\.]+", " ", stem)
    cleaned = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return normalize_ocr_text(cleaned)


def direct_ocr(raw, ext="png"):
    if not (HAS_PYTESSERACT and HAS_TESSERACT_BIN and raw):
        return "", 0.0

    suffix = f".{(ext or 'png').lstrip('.')}"
    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            handle.write(raw)
            temp_path = handle.name

        text = normalize_ocr_text(pytesseract.image_to_string(temp_path, config="--psm 6"))
        confidence = 0.0
        try:
            output = pytesseract.Output.DICT if hasattr(pytesseract, "Output") else None
            if output:
                data = pytesseract.image_to_data(temp_path, config="--psm 6", output_type=output)
                confs = []
                for value in data.get("conf", []):
                    try:
                        parsed = float(value)
                    except Exception:
                        continue
                    if parsed >= 0:
                        confs.append(parsed)
                if confs:
                    confidence = round(sum(confs) / len(confs), 3)
        except Exception:
            confidence = 0.0
        return text, confidence
    except Exception:
        return "", 0.0
    finally:
        if temp_path and os.path.isfile(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def extract(payload):
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return {
            "ok": False,
            "code": "IMAGE_ROWS_REQUIRED",
            "message": "rows are required",
        }

    image_column = str(payload.get("imageColumn") or "").strip()
    target_column = str(payload.get("targetColumn") or "").strip()
    base_dir = str(payload.get("baseDir") or "").strip()
    if not image_column:
        return ok({
            "availability": "blocked",
            "availabilityReason": "Choose an image column before running the image runtime.",
            "requestedRuntime": "opencv-basic",
            "effectiveRuntime": "",
            "requirements": [
                "Select a dataset column that contains image references before running image analysis.",
            ],
            "warnings": [],
            "rows": [],
            "columns": [],
            "featureColumns": [],
            "previewRows": [],
            "labelSummary": [],
            "processedCount": 0,
            "directCount": 0,
            "fallbackCount": 0,
            "failedCount": len(rows),
            "imageStats": {},
        })

    feature_rows = []
    feature_columns = []
    preview_rows = []
    warnings = []
    label_counts = {}
    widths = []
    heights = []
    byte_sizes = []
    processed_count = 0
    direct_count = 0
    fallback_count = 0
    failed_count = 0

    def ensure_feature(row, column, value):
        row[column] = value
        if column not in feature_columns:
            feature_columns.append(column)

    for index, source_row in enumerate(rows):
        if not isinstance(source_row, dict):
            failed_count += 1
            continue

        row = dict(source_row)
        reference = str(source_row.get(image_column) or "").strip()
        if not reference:
            failed_count += 1
            continue

        raw, ext, mime, source_kind = load_reference_bytes(reference, base_dir)
        byte_size = len(raw) if raw else 0
        width = 0
        height = 0
        channels = 0
        mean_intensity = 0.0
        std_intensity = 0.0
        edge_density = 0.0
        runtime = "fallback"

        if target_column:
            label = str(source_row.get(target_column) or "").strip()
            if label:
                label_counts[label] = label_counts.get(label, 0) + 1
        else:
            label = ""

        size = extract_dimensions(raw) if raw else None
        if size:
            width, height = size

        image = decode_with_cv2(raw)
        if image is not None:
            height, width = image.shape[:2]
            channels = image.shape[2] if len(image.shape) == 3 else 1
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if channels > 1 else image
            mean_intensity = float(gray.mean())
            std_intensity = float(gray.std())
            try:
                edges = cv2.Canny(gray, 100, 200)
                edge_density = float((edges > 0).mean())
            except Exception:
                edge_density = 0.0
            runtime = "direct"
            direct_count += 1
        else:
            fallback_count += 1
            if source_kind == "remote":
                warnings.append(f"Row {index + 1}: remote image refs stay in preview mode unless the backend can fetch them.")
            elif source_kind == "missing":
                warnings.append(f"Row {index + 1}: image bytes were not available at '{reference}', so preview fallback features were used.")
            elif source_kind == "inline" and not HAS_CV2:
                warnings.append("OpenCV is not installed, so inline images were converted with manifest fallback features only.")

        processed_count += 1
        hash0, hash1 = hash_features(reference)
        aspect_ratio = round(float(width) / float(height), 6) if width and height else 0.0
        basename = os.path.basename(urllib.parse.urlparse(reference).path or reference)
        normalized_ext = ext or infer_extension(reference, mime)

        ensure_feature(row, f"{image_column}_image_ext", normalized_ext or "")
        ensure_feature(row, f"{image_column}_is_remote", 1 if source_kind == "remote" else 0)
        ensure_feature(row, f"{image_column}_has_inline_data", 1 if source_kind == "inline" else 0)
        ensure_feature(row, f"{image_column}_basename_len", len(basename))
        ensure_feature(row, f"{image_column}_ref_len", len(reference))
        ensure_feature(row, f"{image_column}_byte_size", byte_size)
        ensure_feature(row, f"{image_column}_width", width)
        ensure_feature(row, f"{image_column}_height", height)
        ensure_feature(row, f"{image_column}_aspect_ratio", aspect_ratio)
        ensure_feature(row, f"{image_column}_channels", channels)
        ensure_feature(row, f"{image_column}_hash_0", hash0)
        ensure_feature(row, f"{image_column}_hash_1", hash1)
        if runtime == "direct":
            ensure_feature(row, f"{image_column}_mean_intensity", round(mean_intensity, 6))
            ensure_feature(row, f"{image_column}_std_intensity", round(std_intensity, 6))
            ensure_feature(row, f"{image_column}_edge_density", round(edge_density, 6))

        if width:
            widths.append(width)
        if height:
            heights.append(height)
        if byte_size:
            byte_sizes.append(byte_size)
        if len(preview_rows) < 4:
            preview_rows.append(build_preview_row(reference, label, width, height, byte_size, runtime))
        feature_rows.append(row)

    if not feature_rows:
        return ok({
            "availability": "blocked",
            "availabilityReason": "No usable image references were found in the selected column.",
            "requestedRuntime": "opencv-basic",
            "effectiveRuntime": "",
            "requirements": [
                "Provide data URLs, local files accessible to the backend, or server-relative image paths.",
                "Install opencv-python on the backend to enable pixel-level image feature extraction.",
            ],
            "warnings": warnings[:10],
            "rows": [],
            "columns": [],
            "featureColumns": [],
            "previewRows": [],
            "labelSummary": [],
            "processedCount": 0,
            "directCount": 0,
            "fallbackCount": 0,
            "failedCount": len(rows),
            "imageStats": {},
        })

    requirements = []
    availability = "direct" if direct_count == processed_count and processed_count > 0 else "fallback"
    availability_reason = ""
    if availability == "fallback":
        availability_reason = "OpenCV or image bytes were unavailable for part of the manifest, so fallback reference features were used."
        if not HAS_CV2:
            requirements.append("Install opencv-python and numpy on the backend to enable pixel-level image feature extraction.")

    label_summary = [
        {"label": label, "count": count}
        for label, count in sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    columns = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
    output_columns = []
    seen = set()
    for column in [*columns, *feature_columns]:
        if column in seen:
            continue
        seen.add(column)
        output_columns.append(column)

    return ok({
        "availability": availability,
        "availabilityReason": availability_reason,
        "requestedRuntime": "opencv-basic",
        "effectiveRuntime": "opencv-basic" if availability == "direct" else "manifest-fallback",
        "requirements": requirements,
        "warnings": list(dict.fromkeys(warnings))[:12],
        "rows": feature_rows,
        "columns": output_columns,
        "featureColumns": feature_columns,
        "previewRows": preview_rows,
        "labelSummary": label_summary,
        "processedCount": processed_count,
        "directCount": direct_count,
        "fallbackCount": fallback_count,
        "failedCount": failed_count,
        "imageStats": {
            "avgWidth": round(sum(widths) / len(widths), 3) if widths else 0,
            "avgHeight": round(sum(heights) / len(heights), 3) if heights else 0,
            "avgByteSize": round(sum(byte_sizes) / len(byte_sizes), 3) if byte_sizes else 0,
        },
    })


def ocr(payload):
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return {
            "ok": False,
            "code": "IMAGE_ROWS_REQUIRED",
            "message": "rows are required",
        }

    image_column = str(payload.get("imageColumn") or "").strip()
    target_column = str(payload.get("targetColumn") or "").strip()
    base_dir = str(payload.get("baseDir") or "").strip()
    if not image_column:
        return ok({
            "availability": "blocked",
            "availabilityReason": "Choose an image column before running OCR.",
            "requestedRuntime": "ocr",
            "effectiveRuntime": "",
            "requirements": [
                "Select a dataset column that contains image references before running OCR.",
            ],
            "warnings": [],
            "rows": [],
            "columns": [],
            "textColumn": "",
            "previewRows": [],
            "processedCount": 0,
            "extractedCount": 0,
            "directCount": 0,
            "fallbackCount": 0,
            "failedCount": len(rows),
            "topTokens": [],
            "labelSummary": [],
        })

    text_column = str(payload.get("textColumn") or f"{image_column}_ocr_text").strip()
    text_len_column = f"{text_column}_len"
    token_count_column = f"{text_column}_token_count"
    confidence_column = f"{text_column}_confidence"
    runtime_column = f"{text_column}_runtime"

    feature_rows = []
    preview_rows = []
    warnings = []
    label_counts = {}
    token_counts = {}
    text_lengths = []
    confidences = []
    processed_count = 0
    extracted_count = 0
    direct_count = 0
    fallback_count = 0
    failed_count = 0

    for index, source_row in enumerate(rows):
        if not isinstance(source_row, dict):
            failed_count += 1
            continue

        row = dict(source_row)
        reference = str(source_row.get(image_column) or "").strip()
        if not reference:
            failed_count += 1
            continue

        raw, ext, _mime, source_kind = load_reference_bytes(reference, base_dir)
        text = ""
        confidence = 0.0
        runtime = "blocked"

        if target_column:
            label = str(source_row.get(target_column) or "").strip()
            if label:
                label_counts[label] = label_counts.get(label, 0) + 1
        else:
            label = ""

        direct_text, direct_conf = direct_ocr(raw, ext or "png")
        if direct_text:
            text = direct_text
            confidence = direct_conf
            runtime = "direct"
            direct_count += 1
        else:
            fallback_text = extract_fallback_text(reference)
            if fallback_text:
                text = fallback_text
                runtime = "fallback"
                fallback_count += 1
                if source_kind in ("missing", "remote"):
                    warnings.append(
                        f"Row {index + 1}: OCR fallback used the image reference text because direct OCR bytes were unavailable."
                    )
            else:
                failed_count += 1
                runtime = "blocked"
                if source_kind == "inline":
                    warnings.append(
                        "Inline image OCR needs pytesseract and a Tesseract binary on the backend; no fallback text was available."
                    )
                elif source_kind == "missing":
                    warnings.append(
                        f"Row {index + 1}: OCR could not read '{reference}' because the file was unavailable and no filename text could be derived."
                    )

        normalized_text = normalize_ocr_text(text)
        tokens = [token for token in re.split(r"\s+", normalized_text.lower()) if token]
        if normalized_text:
            extracted_count += 1
            text_lengths.append(len(normalized_text))
            if confidence > 0:
                confidences.append(confidence)
            for token in tokens:
                token_counts[token] = token_counts.get(token, 0) + 1

        row[text_column] = normalized_text
        row[text_len_column] = len(normalized_text)
        row[token_count_column] = len(tokens)
        row[confidence_column] = round(confidence, 3) if confidence else 0
        row[runtime_column] = runtime
        feature_rows.append(row)
        processed_count += 1

        if len(preview_rows) < 4:
            preview_rows.append({
                "imageRef": reference,
                "label": label,
                "text": normalized_text,
                "confidence": round(confidence, 3) if confidence else 0,
                "runtime": runtime,
            })

    if extracted_count <= 0:
        requirements = [
            "Provide image references with readable filenames or install pytesseract plus a Tesseract OCR binary on the backend.",
        ]
        if not HAS_PYTESSERACT or not HAS_TESSERACT_BIN:
            requirements.append("Install pytesseract and the Tesseract OCR binary on the backend to enable direct OCR.")
        return ok({
            "availability": "blocked",
            "availabilityReason": "No OCR text could be extracted from the selected image references.",
            "requestedRuntime": "ocr",
            "effectiveRuntime": "",
            "requirements": requirements,
            "warnings": list(dict.fromkeys(warnings))[:12],
            "rows": [],
            "columns": [],
            "textColumn": text_column,
            "previewRows": preview_rows,
            "processedCount": processed_count,
            "extractedCount": 0,
            "directCount": direct_count,
            "fallbackCount": fallback_count,
            "failedCount": failed_count,
            "topTokens": [],
            "labelSummary": [
                {"label": label, "count": count}
                for label, count in sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
        })

    requirements = []
    availability = "direct" if direct_count == extracted_count and extracted_count > 0 else "fallback"
    availability_reason = ""
    if availability == "fallback":
        availability_reason = "Direct OCR was not available for all rows, so fallback text from the image reference was used where needed."
        if not HAS_PYTESSERACT or not HAS_TESSERACT_BIN:
            requirements.append("Install pytesseract and the Tesseract OCR binary on the backend to enable direct OCR.")

    columns = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
    output_columns = []
    seen = set()
    for column in [*columns, text_column, text_len_column, token_count_column, confidence_column, runtime_column]:
        if column in seen:
            continue
        seen.add(column)
        output_columns.append(column)

    top_tokens = [
        {"token": token, "count": count}
        for token, count in sorted(token_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    ]

    return ok({
        "availability": availability,
        "availabilityReason": availability_reason,
        "requestedRuntime": "ocr",
        "effectiveRuntime": "tesseract" if availability == "direct" else "reference-fallback",
        "requirements": requirements,
        "warnings": list(dict.fromkeys(warnings))[:12],
        "rows": feature_rows,
        "columns": output_columns,
        "textColumn": text_column,
        "previewRows": preview_rows,
        "processedCount": processed_count,
        "extractedCount": extracted_count,
        "directCount": direct_count,
        "fallbackCount": fallback_count,
        "failedCount": failed_count,
        "topTokens": top_tokens,
        "labelSummary": [
            {"label": label, "count": count}
            for label, count in sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "textStats": {
            "avgTextLength": round(sum(text_lengths) / len(text_lengths), 3) if text_lengths else 0,
            "avgConfidence": round(sum(confidences) / len(confidences), 3) if confidences else 0,
        },
    })


def main():
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError:
        out({"ok": False, "code": "INVALID_JSON", "message": "invalid json"})
        return

    if not isinstance(payload, dict):
        out({"ok": False, "code": "INVALID_PAYLOAD", "message": "payload must be an object"})
        return

    op = str(payload.get("op") or "extract").strip().lower()
    if op == "capabilities":
        out(capabilities())
        return
    if op == "ocr":
        out(ocr(payload))
        return
    if op != "extract":
        out({"ok": False, "code": "IMAGE_OP_INVALID", "message": "unsupported op"})
        return
    out(extract(payload))


if __name__ == "__main__":
    main()
