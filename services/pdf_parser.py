import re
from io import BytesIO
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dateutil.parser import parse as date_parse


DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b",
]

AMOUNT_PATTERN = re.compile(
    r"(?P<currency>[$€£])?\s*(?P<amount>(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{2})?)"
)


def _normalize_amount(raw: str) -> float:
    cleaned = raw.replace(",", "").strip()
    return float(cleaned)


def _normalize_date(raw: str) -> str:
    # dayfirst=True handles common dd/mm/yyyy formats.
    dt = date_parse(raw, dayfirst=True, fuzzy=True)
    return dt.strftime("%Y-%m-%d")


def _extract_expenses_from_lines(lines: List[str]) -> List[Dict[str, Any]]:
    date_regexes = [re.compile(p) for p in DATE_PATTERNS]
    amount_regex = AMOUNT_PATTERN

    date_hits: List[Tuple[int, str]] = []
    amount_hits: List[Tuple[int, float]] = []

    for i, line in enumerate(lines):
        for dr in date_regexes:
            m = dr.search(line)
            if m:
                try:
                    date_hits.append((i, _normalize_date(m.group(0))))
                except Exception:
                    pass
                break

        am = amount_regex.search(line)
        if am:
            try:
                amount_hits.append((i, _normalize_amount(am.group("amount"))))
            except Exception:
                pass

    expenses: List[Dict[str, Any]] = []

    def safe_vendor(line: str) -> str:
        # Heuristic: keep the first chunk of words (avoid repeating dates/amounts).
        chunk = re.sub(r"\s+", " ", line).strip()
        chunk = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "", chunk)
        chunk = re.sub(r"\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b", "", chunk)
        chunk = amount_regex.sub("", chunk)
        chunk = chunk.strip(" -,:;|")
        return chunk[:60] if chunk else "Unknown Vendor"

    # Pair each date with the nearest amount (within a small window).
    for di, d in date_hits:
        candidates = [(ai, amt) for (ai, amt) in amount_hits if abs(ai - di) <= 2]
        if candidates:
            ai, amt = sorted(candidates, key=lambda x: abs(x[0] - di))[0]
            vendor = safe_vendor(lines[di - 1]) if di - 1 >= 0 else safe_vendor(lines[di])
            expenses.append(
                {
                    "amount": float(amt),
                    "date": d,
                    "vendor": vendor,
                    "category": None,
                }
            )

    # Fallback: if no date/amount pairing, create expenses based only on amounts.
    if not expenses and amount_hits:
        # Try to find a single date in the whole text, otherwise use today's date.
        full_text = "\n".join(lines)
        chosen_date: Optional[str] = None
        for dr in date_regexes:
            m = dr.search(full_text)
            if m:
                try:
                    chosen_date = _normalize_date(m.group(0))
                except Exception:
                    chosen_date = None
                break

        for _, amt in amount_hits[:50]:
            expenses.append(
                {
                    "amount": float(amt),
                    "date": chosen_date or datetime.utcnow().strftime("%Y-%m-%d"),
                    "vendor": "Unknown Vendor",
                    "category": None,
                }
            )

    # Basic de-duplication (same date+amount+vendor)
    seen = set()
    unique: List[Dict[str, Any]] = []
    for e in expenses:
        key = (e["date"], float(e["amount"]), e["vendor"].strip().lower())
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)

    return unique


def parse_pdf_bytes_pymupdf(file_bytes: bytes) -> List[Dict[str, Any]]:
    try:
        import fitz  # PyMuPDF
    except Exception as e:
        raise RuntimeError(
            "PyMuPDF is not installed or failed to import. Install with `pip install pymupdf`."
        ) from e

    with fitz.open(stream=file_bytes, filetype="pdf") as doc:
        text_chunks: List[str] = []
        for page in doc:
            text_chunks.append(page.get_text("text"))
        text = "\n".join(text_chunks)

    # Parse by lines for better heuristics.
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []

    return _extract_expenses_from_lines(lines)


def parse_pdf_bytes_azure_document_intelligence(
    file_bytes: bytes, endpoint: str, api_key: str
) -> List[Dict[str, Any]]:
    if not endpoint or not api_key:
        raise ValueError("Azure Document Intelligence endpoint/key missing.")
    try:
        from azure.ai.formrecognizer import DocumentAnalysisClient
        from azure.core.credentials import AzureKeyCredential
    except Exception as e:
        raise RuntimeError(
            "azure-ai-formrecognizer package is required for Azure parser."
        ) from e

    client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))
    poller = client.begin_analyze_document("prebuilt-invoice", document=BytesIO(file_bytes))
    result = poller.result()

    out: List[Dict[str, Any]] = []
    for doc in result.documents:
        fields = doc.fields or {}
        vendor = None
        amount = None
        date = None

        vendor_field = fields.get("VendorName") or fields.get("MerchantName")
        if vendor_field and getattr(vendor_field, "value", None):
            vendor = str(vendor_field.value)

        amount_field = fields.get("InvoiceTotal") or fields.get("TotalAmount") or fields.get("AmountDue")
        if amount_field and getattr(amount_field, "value", None) is not None:
            try:
                amount = float(amount_field.value)
            except Exception:
                amount = None

        date_field = fields.get("InvoiceDate") or fields.get("TransactionDate") or fields.get("DueDate")
        if date_field and getattr(date_field, "value", None):
            try:
                date = _normalize_date(str(date_field.value))
            except Exception:
                date = None

        if amount is not None:
            out.append(
                {
                    "amount": amount,
                    "date": date or datetime.utcnow().strftime("%Y-%m-%d"),
                    "vendor": vendor or "Unknown Vendor",
                    "category": None,
                }
            )

    return out


def parse_pdf_bytes(
    file_bytes: bytes,
    parser_type: str = "pymupdf",
    azure_endpoint: Optional[str] = None,
    azure_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Modular PDF parsing entrypoint.
    Supports multiple backends later (e.g., Azure Document Intelligence).
    """

    parser_type = (parser_type or "pymupdf").lower().strip()
    if parser_type == "pymupdf":
        return parse_pdf_bytes_pymupdf(file_bytes)

    if parser_type in {"azure", "document_intelligence", "di"}:
        try:
            azure_rows = parse_pdf_bytes_azure_document_intelligence(
                file_bytes=file_bytes,
                endpoint=azure_endpoint or "",
                api_key=azure_key or "",
            )
            if azure_rows:
                return azure_rows
        except Exception:
            # Production fallback path: do not fail the request if Azure parser fails.
            pass
        return parse_pdf_bytes_pymupdf(file_bytes)

    raise ValueError(f"Unsupported parser_type={parser_type}")

