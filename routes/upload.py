import csv
import io
from pathlib import Path
from typing import Any, Dict, List, Optional

from dateutil.parser import parse as date_parse
from flask import Blueprint, current_app, jsonify, request

from services.ml_model import AutoCategorizer
from services.pdf_parser import parse_pdf_bytes
from services.storage import Expense, FileStorageAdapter


upload_bp = Blueprint("upload", __name__)
ALLOWED_EXTENSIONS = {".csv", ".pdf"}


def _parse_amount(raw: Any) -> float:
    if raw is None:
        raise ValueError("Missing amount")
    s = str(raw).strip()
    if not s:
        raise ValueError("Empty amount")
    s = s.replace(",", "")
    # remove currency symbols
    for sym in ["$", "€", "£"]:
        s = s.replace(sym, "")
    return float(s)


def _parse_date(raw: Any) -> str:
    if raw is None:
        raise ValueError("Missing date")
    s = str(raw).strip()
    if not s:
        raise ValueError("Empty date")
    dt = date_parse(s, dayfirst=True, fuzzy=True)
    return dt.strftime("%Y-%m-%d")


def _detect_and_parse_csv(file_bytes: bytes) -> List[Dict[str, Any]]:
    decoded = file_bytes.decode("utf-8-sig", errors="replace")
    if not decoded.strip():
        return []

    # Try delimiter sniffing.
    sample = decoded[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
    except Exception:
        dialect = csv.get_dialect("excel")

    reader = csv.DictReader(io.StringIO(decoded), dialect=dialect)
    if not reader.fieldnames:
        raise ValueError("CSV header not found. Please include column headers.")

    # Normalize header names -> canonical fields.
    header_map: Dict[str, str] = {}
    for h in reader.fieldnames:
        key = str(h).strip().lower()
        if key in {"amount", "total", "expense", "value"}:
            header_map[h] = "amount"
        elif key in {"date", "expense_date", "transaction_date", "day"}:
            header_map[h] = "date"
        elif key in {"category", "cat"}:
            header_map[h] = "category"
        elif key in {"vendor", "merchant", "description", "store"}:
            header_map[h] = "vendor"

    # If required fields aren't present, fail early with a clear message.
    required_fields = {"amount", "date"}
    mapped_fields = set(header_map.values())
    if not required_fields.issubset(mapped_fields):
        raise ValueError(
            "CSV must include at least `amount` and `date` columns (case-insensitive)."
        )

    expenses: List[Dict[str, Any]] = []
    for row in reader:
        # Skip empty rows
        if not any(v and str(v).strip() for v in row.values()):
            continue

        def get_canonical(field: str) -> Optional[str]:
            for raw_h, canon in header_map.items():
                if canon == field:
                    val = row.get(raw_h)
                    return val
            return None

        amount_raw = get_canonical("amount")
        date_raw = get_canonical("date")
        category_raw = get_canonical("category")
        vendor_raw = get_canonical("vendor")

        if amount_raw is None or date_raw is None:
            continue

        try:
            amount = _parse_amount(amount_raw)
            date = _parse_date(date_raw)
        except Exception:
            # Skip invalid rows.
            continue

        expenses.append(
            {
                "amount": amount,
                "date": date,
                "category": (str(category_raw).strip() if category_raw is not None else None)
                or None,
                "vendor": (str(vendor_raw).strip() if vendor_raw is not None else None)
                or "Unknown Vendor",
            }
        )

    return expenses


def _categorize_and_build_expenses(
    raw_expenses: List[Dict[str, Any]],
    repo_source: str,
    upload_url: Optional[str] = None,
) -> List[Expense]:
    categorizer: AutoCategorizer = current_app.extensions["categorizer"]
    repo = current_app.extensions["repo"]
    historical_expenses = repo.list_expenses(limit=5000)
    built: List[Expense] = []
    for e in raw_expenses:
        amount = float(e["amount"])
        date = e["date"]
        vendor = str(e.get("vendor") or "Unknown Vendor").strip()
        category_raw = e.get("category")
        category = categorizer.categorize(
            amount=amount,
            category=category_raw,
            vendor=vendor,
            historical_expenses=historical_expenses,
        )
        built.append(
            Expense(
                amount=amount,
                category=category,
                expense_date=date,
                vendor=vendor,
                source=repo_source,
                upload_url=upload_url,
            )
        )
    return built


def _validate_upload(file_name: str, file_bytes: bytes, expected_ext: str) -> Optional[str]:
    ext = Path(file_name or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return "Unsupported file type. Only CSV and PDF are allowed."
    if ext != expected_ext:
        return f"Invalid file type for this endpoint. Expected `{expected_ext}`."
    max_bytes = int(current_app.config.get("MAX_UPLOAD_SIZE_BYTES", 10 * 1024 * 1024))
    if len(file_bytes) > max_bytes:
        return f"File too large. Max allowed size is {max_bytes // (1024 * 1024)}MB."
    return None


@upload_bp.route("/csv", methods=["POST"])
def upload_csv():
    repo = current_app.extensions["repo"]
    file_storage: Optional[FileStorageAdapter] = current_app.extensions.get("file_storage")

    if "file" not in request.files:
        return jsonify({"error": "Missing `file` in form-data."}), 400

    f = request.files["file"]
    if not f or not f.filename:
        return jsonify({"error": "Invalid CSV file."}), 400

    file_bytes = f.read()
    if not file_bytes:
        return jsonify({"error": "Empty file uploaded."}), 400
    validation_error = _validate_upload(f.filename, file_bytes, expected_ext=".csv")
    if validation_error:
        return jsonify({"error": validation_error}), 400

    # Optional: store uploaded file locally to keep extendable to Azure Blob later.
    upload_url = None
    if current_app.config.get("STORE_UPLOADS", True) and file_storage:
        try:
            upload_url = file_storage.save(file_bytes, f.filename)
        except Exception:
            pass

    try:
        raw_expenses = _detect_and_parse_csv(file_bytes)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    if not raw_expenses:
        return jsonify({"error": "No valid expense rows found in CSV."}), 400

    expenses = _categorize_and_build_expenses(raw_expenses, repo_source="csv", upload_url=upload_url)
    inserted = repo.add_expenses(expenses)

    return jsonify(
        {
            "status": "ok",
            "insertedCount": inserted,
            "message": "CSV uploaded successfully.",
            "uploadUrl": upload_url,
        }
    )


@upload_bp.route("/pdf", methods=["POST"])
def upload_pdf():
    repo = current_app.extensions["repo"]
    file_storage: Optional[FileStorageAdapter] = current_app.extensions.get("file_storage")
    pdf_parser_type = current_app.config.get("PDF_PARSER", "pymupdf")

    if "file" not in request.files:
        return jsonify({"error": "Missing `file` in form-data."}), 400

    f = request.files["file"]
    if not f or not f.filename:
        return jsonify({"error": "Invalid PDF file."}), 400

    file_bytes = f.read()
    if not file_bytes:
        return jsonify({"error": "Empty file uploaded."}), 400
    validation_error = _validate_upload(f.filename, file_bytes, expected_ext=".pdf")
    if validation_error:
        return jsonify({"error": validation_error}), 400

    upload_url = None
    if current_app.config.get("STORE_UPLOADS", True) and file_storage:
        try:
            upload_url = file_storage.save(file_bytes, f.filename)
        except Exception:
            pass

    try:
        raw_expenses = parse_pdf_bytes(
            file_bytes,
            parser_type=pdf_parser_type,
            azure_endpoint=current_app.config.get("AZURE_DOC_INTELLIGENCE_ENDPOINT", ""),
            azure_key=current_app.config.get("AZURE_DOC_INTELLIGENCE_KEY", ""),
        )
    except NotImplementedError as e:
        return jsonify({"error": str(e)}), 501
    except Exception as e:
        return jsonify({"error": "Failed to parse PDF."}), 400

    if not raw_expenses:
        return jsonify({"error": "No expenses could be extracted from the PDF."}), 400

    expenses = _categorize_and_build_expenses(raw_expenses, repo_source="pdf", upload_url=upload_url)
    inserted = repo.add_expenses(expenses)

    return jsonify(
        {
            "status": "ok",
            "insertedCount": inserted,
            "message": "PDF uploaded and parsed successfully.",
            "uploadUrl": upload_url,
        }
    )


@upload_bp.route("/manual", methods=["POST"])
def upload_manual():
    repo = current_app.extensions["repo"]
    payload = request.get_json(silent=True) or {}

    amount_raw = payload.get("amount")
    date_raw = payload.get("date")
    vendor_raw = payload.get("vendor")
    category_raw = payload.get("category")

    try:
        amount = _parse_amount(amount_raw)
        expense_date = _parse_date(date_raw)
    except Exception as e:
        return jsonify({"error": f"Invalid input: {e}"}), 400

    raw = [
        {
            "amount": amount,
            "date": expense_date,
            "category": category_raw,
            "vendor": vendor_raw or "Unknown Vendor",
        }
    ]

    expenses = _categorize_and_build_expenses(raw, repo_source="manual")
    inserted = repo.add_expenses(expenses)

    return jsonify(
        {
            "status": "ok",
            "insertedCount": inserted,
            "message": "Manual expense saved successfully.",
        }
    )

