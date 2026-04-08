import csv
import io
from pathlib import Path
from typing import Any, Dict, List, Optional

from dateutil.parser import parse as date_parse
from flask import Blueprint, current_app, jsonify, request

from services.ml_model import AutoCategorizer
from services.pdf_parser import parse_pdf_bytes
from services.storage import Expense


upload_bp = Blueprint("upload", __name__)
ALLOWED_EXTENSIONS = {".csv", ".pdf"}


# -------------------------
# HELPERS
# -------------------------

def _parse_amount(raw: Any) -> float:
    if raw is None:
        raise ValueError("Missing amount")
    s = str(raw).strip()
    if not s:
        raise ValueError("Empty amount")
    s = s.replace(",", "")
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


def _remove_duplicates(raw_expenses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate expenses safely."""
    unique = set()
    cleaned = []

    for e in raw_expenses:
        key = (e["amount"], e["date"], e.get("vendor"))
        if key not in unique:
            unique.add(key)
            cleaned.append(e)

    return cleaned


# -------------------------
# CSV PARSER
# -------------------------

def _detect_and_parse_csv(file_bytes: bytes) -> List[Dict[str, Any]]:
    decoded = file_bytes.decode("utf-8-sig", errors="replace")
    if not decoded.strip():
        return []

    sample = decoded[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
    except Exception:
        dialect = csv.get_dialect("excel")

    reader = csv.DictReader(io.StringIO(decoded), dialect=dialect)
    if not reader.fieldnames:
        raise ValueError("CSV header not found.")

    header_map: Dict[str, str] = {}
    for h in reader.fieldnames:
        key = str(h).strip().lower()
        if key in {"amount", "total", "expense", "value"}:
            header_map[h] = "amount"
        elif key in {"date", "transaction_date", "day"}:
            header_map[h] = "date"
        elif key in {"category", "cat"}:
            header_map[h] = "category"
        elif key in {"vendor", "merchant", "description"}:
            header_map[h] = "vendor"

    if not {"amount", "date"}.issubset(set(header_map.values())):
        raise ValueError("CSV must include amount and date columns.")

    expenses = []

    for row in reader:
        if not any(v and str(v).strip() for v in row.values()):
            continue

        def get_field(field):
            for raw_h, canon in header_map.items():
                if canon == field:
                    return row.get(raw_h)
            return None

        try:
            amount = _parse_amount(get_field("amount"))
            date = _parse_date(get_field("date"))
        except:
            continue

        expenses.append({
            "amount": amount,
            "date": date,
            "category": get_field("category"),
            "vendor": get_field("vendor") or "Unknown Vendor",
        })

    return expenses


# -------------------------
# CORE BUILDER
# -------------------------

def _categorize_and_build_expenses(raw_expenses, source, upload_url=None):
    categorizer: AutoCategorizer = current_app.extensions["categorizer"]
    repo = current_app.extensions["repo"]

    historical = repo.list_expenses()
    built = []

    for e in raw_expenses:
        category = categorizer.categorize(
            amount=e["amount"],
            category=e.get("category"),
            vendor=e.get("vendor"),
            historical_expenses=historical,
        )

        built.append(
            Expense(
                amount=e["amount"],
                category=category,
                expense_date=e["date"],
                vendor=e.get("vendor"),
                source=source,
                upload_url=upload_url,
            )
        )

    return built


# -------------------------
# VALIDATION
# -------------------------

def _validate_upload(file_name, file_bytes, expected_ext):
    ext = Path(file_name or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return "Only CSV/PDF allowed"
    if ext != expected_ext:
        return f"Expected {expected_ext}"
    return None


# -------------------------
# ROUTES
# -------------------------

@upload_bp.route("/csv", methods=["POST"])
def upload_csv():
    repo = current_app.extensions["repo"]
    file_storage = current_app.extensions.get("file_storage")

    if "file" not in request.files:
        return jsonify({"error": "Missing file"}), 400

    f = request.files["file"]
    file_bytes = f.read()

    error = _validate_upload(f.filename, file_bytes, ".csv")
    if error:
        return jsonify({"error": error}), 400

    try:
        raw_expenses = _detect_and_parse_csv(file_bytes)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    if not raw_expenses:
        return jsonify({"error": "No valid data"}), 400

    # Auto-clean: new file upload starts a fresh dataset
    repo.clear_all()

    # ✅ REMOVE DUPLICATES (CORRECT)
    raw_expenses = _remove_duplicates(raw_expenses)

    upload_url = None
    if file_storage:
        try:
            upload_url = file_storage.save(file_bytes, f.filename)
        except Exception as e:
            print("File upload error:", e)

    expenses = _categorize_and_build_expenses(raw_expenses, "csv", upload_url)
    inserted = repo.add_expenses(expenses)
    print("📥 RAW:", raw_expenses)
    print("📊 FINAL:", inserted)
    print("💾 INSERTED:", inserted)
    

    return jsonify({"status": "ok", "inserted": inserted})


@upload_bp.route("/pdf", methods=["POST"])
def upload_pdf():
    repo = current_app.extensions["repo"]
    file_storage = current_app.extensions.get("file_storage")

    if "file" not in request.files:
        return jsonify({"error": "Missing file"}), 400

    f = request.files["file"]
    file_bytes = f.read()

    error = _validate_upload(f.filename, file_bytes, ".pdf")
    if error:
        return jsonify({"error": error}), 400

    try:
        raw_expenses = parse_pdf_bytes(file_bytes)
    except Exception:
        return jsonify({"error": "PDF parse failed"}), 400

    if not raw_expenses:
        return jsonify({"error": "No data extracted"}), 400

    # Auto-clean: new file upload starts a fresh dataset
    repo.clear_all()

    # ✅ REMOVE DUPLICATES
    raw_expenses = _remove_duplicates(raw_expenses)

    upload_url = None
    if file_storage:
        try:
            upload_url = file_storage.save(file_bytes, f.filename)
        except Exception as e:
            print("File upload error:", e)

    expenses = _categorize_and_build_expenses(raw_expenses, "pdf", upload_url)
    inserted = repo.add_expenses(expenses)

    print("📥 RAW:", raw_expenses)
    print("📊 FINAL:", expenses)
    print("💾 INSERTED:", inserted)

    return jsonify({"status": "ok", "inserted": inserted})


@upload_bp.route("/manual", methods=["POST"])
def upload_manual():
    repo = current_app.extensions["repo"]
    payload = request.get_json(silent=True) or {}

    try:
        raw = [{
            "amount": _parse_amount(payload.get("amount")),
            "date": _parse_date(payload.get("date")),
            "category": payload.get("category"),
            "vendor": payload.get("vendor"),
        }]
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    expenses = _categorize_and_build_expenses(raw, "manual")
    repo.add_expenses(expenses)
    return jsonify({"status": "ok"})