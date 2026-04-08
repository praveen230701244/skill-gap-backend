import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from services.vendor_normalizer import normalize_vendor

try:
    from dateutil.parser import parse as date_parse  # type: ignore
except Exception:  # pragma: no cover
    date_parse = None


# -------------------------
# Regex primitives
# -------------------------

# Currency and amount patterns (supports ₹, Rs, INR, $, etc.)
_CURRENCY_PREFIX = r"(?:₹|rs\.?|inr|usd|\$|eur|€|gbp|£)\s*"
_NUMBER = r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?"
_AMOUNT_RE = re.compile(
    rf"(?P<prefix>{_CURRENCY_PREFIX})?(?P<num>{_NUMBER})",
    flags=re.IGNORECASE,
)

_DATE_RE = re.compile(
    r"(?P<d>\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b)|"
    r"(?P<ymd>\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b)|"
    r"(?P<mon>\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4}\b)|"
    r"(?P<mon2>\b[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}\b)"
)

# UPI / wallet / transfer narration patterns
_UPI_TO_RE = re.compile(
    r"\bupi\b.*?\b(?:pay(?:ment)?|txn|transfer)?\b.*?\b(?:to|@)\s*(?P<vendor>[A-Za-z0-9&.\- _]{2,})",
    flags=re.IGNORECASE,
)
_PAID_TO_RE = re.compile(r"\bpaid\s+to\s+(?P<vendor>.+)$", flags=re.IGNORECASE)
_TO_RE = re.compile(r"\bto\s+(?P<vendor>[A-Za-z][A-Za-z0-9&.\- _]{1,})$", flags=re.IGNORECASE)

# Statement-like lines often have multiple columns separated by large gaps / tabs.
_COL_SPLIT_RE = re.compile(r"(?:\t+| {2,})")

# Strong noise words that should not become vendors.
_SKIP_LINE_RE = re.compile(
    r"\b("
    r"grand\s+total|sub\s*total|total\s+amount|amount\s+due|"
    r"tax|gst|cgst|sgst|igst|invoice|bill\s+to|ship\s+to|"
    r"opening\s+balance|closing\s+balance|balance\s+forward|"
    r"statement\s+summary|page\s+\d+\s+of\s+\d+"
    r")\b",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class _ParsedTxn:
    amount: float
    vendor: str
    date: str


def _safe_parse_date(text: str) -> Optional[str]:
    """
    Return ISO date (YYYY-MM-DD) if detected, else None.
    Uses dateutil if available; otherwise falls back to regex-only extraction.
    """
    if not text:
        return None

    m = _DATE_RE.search(text)
    if not m:
        return None

    raw = m.group(0)
    if date_parse:
        try:
            dt = date_parse(raw, dayfirst=True, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    # Regex-only fallback: dd/mm/yyyy or yyyy-mm-dd
    raw2 = raw.replace(".", "/")
    parts = re.split(r"[-/]", raw2)
    try:
        if len(parts[0]) == 4:  # yyyy-mm-dd
            y, mo, d = int(parts[0]), int(parts[1]), int(parts[2])
        else:  # dd/mm/yyyy
            d, mo, y = int(parts[0]), int(parts[1]), int(parts[2])
            if y < 100:
                y += 2000
        return datetime(y, mo, d).strftime("%Y-%m-%d")
    except Exception:
        return None


def _amount_candidates(text: str) -> List[Tuple[float, int]]:
    """
    Extract numeric amount candidates with their position.
    Returns: [(amount, end_index), ...]
    """
    out: List[Tuple[float, int]] = []
    for m in _AMOUNT_RE.finditer(text or ""):
        raw = m.group("num")
        try:
            val = float(raw.replace(",", ""))
        except Exception:
            continue
        out.append((val, m.end()))
    return out


def _pick_amount(text: str) -> Optional[float]:
    """
    Heuristics to pick the transaction amount in messy receipts / statements.
    - Prefer the last amount on the line (common for receipts).
    - If the line looks like a bank statement with balance, avoid the final "balance" token when detected.
    """
    s = (text or "").strip()
    if not s:
        return None

    cands = _amount_candidates(s)
    if not cands:
        return None

    s_low = s.lower()

    # Statement-like rows may contain multiple numeric columns: debit, credit, balance.
    # If we see both debit/credit cues, prefer debit-looking amount.
    if len(cands) >= 2 and any(k in s_low for k in ["debit", "dr"]):
        # Debit is often earlier than balance but not always. If "balance" exists, use the one before it.
        if ("bal" in s_low or "balance" in s_low) and len(cands) >= 2:
            return cands[-2][0]
        return cands[-1][0]
    if len(cands) >= 2 and any(k in s_low for k in ["credit", "cr", "refund", "reversal"]):
        # Many expense analyzers treat credits as non-expense; return None to skip.
        return None

    # If this line explicitly labels debit/withdrawal, keep last.
    if any(k in s_low for k in ["debit", "dr", "withdrawal", "paid", "purchase"]):
        return cands[-1][0]

    # If it contains "bal"/"balance" and has multiple amounts, drop the last (often balance).
    if ("bal" in s_low or "balance" in s_low) and len(cands) >= 2:
        return cands[-2][0]

    # Generic: last amount.
    return cands[-1][0]


def _extract_vendor(text: str) -> str:
    """
    Extract a likely merchant/vendor string from a transaction-like line.
    This returns a cleaned/normalized merchant name, not a raw token.
    """
    raw = (text or "").strip()
    if not raw:
        return "Unknown"

    # UPI / wallet patterns (high confidence)
    m = _UPI_TO_RE.search(raw)
    if m:
        v = normalize_vendor(m.group("vendor"))
        return v

    m = _PAID_TO_RE.search(raw)
    if m:
        v = normalize_vendor(m.group("vendor"))
        return v

    # If we have a "to <vendor>" ending, it often indicates a payee.
    m = _TO_RE.search(raw)
    if m:
        v = normalize_vendor(m.group("vendor"))
        return v

    # Statement-like rows: date | narration | debit | credit | balance
    cols = [c.strip() for c in _COL_SPLIT_RE.split(raw) if c.strip()]
    if len(cols) >= 3:
        # Usually the narration is the widest non-date, non-amount column.
        non_amount_cols = []
        for c in cols:
            if _DATE_RE.search(c):
                continue
            if _AMOUNT_RE.search(c) and len(c) <= 20:
                continue
            non_amount_cols.append(c)
        if non_amount_cols:
            # Pick the longest narration-ish column.
            non_amount_cols.sort(key=len, reverse=True)
            v = normalize_vendor(non_amount_cols[0])
            return v

    # Fallback: remove amount and date fragments and normalize what remains.
    stripped = _DATE_RE.sub(" ", raw)
    stripped = _AMOUNT_RE.sub(" ", stripped)
    v = normalize_vendor(stripped)

    # Never allow vendors that are clearly noise tokens
    bad = {
        "unknown",
        "date",
        "payment",
        "upi",
        "txn",
        "txnid",
        "ref",
        "id",
        "success",
        "transfer",
        "debit",
        "credit",
    }
    if not v or v.strip().lower() in bad:
        return "Unknown"
    return v


def _is_plausible_txn_line(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return False
    if _SKIP_LINE_RE.search(s):
        return False
    # Needs at least one amount.
    if not _AMOUNT_RE.search(s):
        return False
    # Avoid lines that are mostly punctuation or too short.
    if len(re.sub(r"[\W_]+", "", s)) < 4:
        return False
    return True


def _lines_from_page(page: Any) -> List[str]:
    """
    Reconstruct reading-order lines from a PDF page using word positions.
    This is much more robust for multi-column bank statements than page.get_text().
    """
    try:
        words = page.get_text("words")  # [x0, y0, x1, y1, "word", block, line, word_no]
    except Exception:
        words = None

    if not words:
        text = page.get_text() or ""
        return [l.strip() for l in text.split("\n") if l.strip()]

    # Group words into lines by y coordinate (with tolerance)
    rows: List[Tuple[float, List[Tuple[float, str]]]] = []
    y_tol = 2.5
    for w in words:
        x0, y0, _x1, _y1, txt = w[0], w[1], w[2], w[3], w[4]
        t = str(txt).strip()
        if not t:
            continue
        placed = False
        for i, (yy, items) in enumerate(rows):
            if abs(yy - y0) <= y_tol:
                items.append((x0, t))
                placed = True
                break
        if not placed:
            rows.append((y0, [(x0, t)]))

    rows.sort(key=lambda r: r[0])
    out_lines: List[str] = []
    for _y, items in rows:
        items.sort(key=lambda it: it[0])
        line = " ".join(t for _x, t in items).strip()
        if line:
            out_lines.append(line)
    return out_lines


def _extract_transactions(lines: Iterable[str]) -> List[_ParsedTxn]:
    txns: List[_ParsedTxn] = []
    last_seen_date: Optional[str] = None

    for line in lines:
        s = (line or "").strip()
        if not s:
            continue

        # GPay/PhonePe exports sometimes have status tokens like "Success" in their own column.
        if s.lower() == "success":
            continue
        if re.search(r"\bsuccess\b", s, flags=re.IGNORECASE):
            # Remove it so vendor extraction prefers the actual description.
            s = re.sub(r"\bsuccess\b", " ", s, flags=re.IGNORECASE)
            s = re.sub(r"\s+", " ", s).strip()

        if not _is_plausible_txn_line(s):
            # Still update last_seen_date from context lines.
            d = _safe_parse_date(s)
            if d:
                last_seen_date = d
            continue

        amount = _pick_amount(s)
        if amount is None:
            continue

        if amount <= 0 or amount > 5_000_000:
            continue

        d = _safe_parse_date(s) or last_seen_date
        if not d:
            d = datetime.utcnow().strftime("%Y-%m-%d")

        vendor = _extract_vendor(s)
        if not vendor or str(vendor).strip().lower() in {"unknown", "success", "payment", "upi", "txn", "ref", "id"}:
            vendor = "Unknown"

        txns.append(_ParsedTxn(amount=float(amount), vendor=vendor, date=d))

    return txns


def parse_pdf_bytes(file_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Advanced PDF transaction extraction.
    Keeps output schema stable: [{"amount": float, "date": "YYYY-MM-DD", "vendor": str, "category": None}, ...]
    """
    try:
        import fitz  # type: ignore
    except Exception as e:
        raise RuntimeError("PyMuPDF not installed. Run: pip install pymupdf") from e

    doc = fitz.open(stream=file_bytes, filetype="pdf")

    all_lines: List[str] = []
    for page in doc:
        all_lines.extend(_lines_from_page(page))

    txns = _extract_transactions(all_lines)

    # Remove duplicates conservatively (amount+date+vendor)
    seen = set()
    final: List[Dict[str, Any]] = []
    for t in txns:
        key = (round(t.amount, 2), t.date, t.vendor)
        if key in seen:
            continue
        seen.add(key)
        final.append(
            {
                "amount": float(t.amount),
                "date": t.date,
                "vendor": t.vendor,
                "category": None,
            }
        )

    if not final:
        print("⚠️ No items extracted from PDF")

    return final