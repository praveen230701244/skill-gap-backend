import re
from typing import Iterable


_NOISE_TOKENS = {
    "upi",
    "imps",
    "neft",
    "rtgs",
    "ach",
    "nach",
    "ecs",
    "txn",
    "txnid",
    "ref",
    "rrn",
    "utr",
    "id",
    "payment",
    "pay",
    "paid",
    "transfer",
    "to",
    "from",
    "via",
    "pos",
    "debit",
    "credit",
    "dr",
    "cr",
    "card",
    "visa",
    "mastercard",
    "rupay",
    "bank",
    "a/c",
    "ac",
    "account",
    "mobile",
    "wallet",
    "bill",
    "invoice",
    "order",
    "online",
    "netbanking",
    "net",
    "success",
    "date",
}

_LEGAL_SUFFIXES = {
    "ltd",
    "limited",
    "pvt",
    "private",
    "llp",
    "inc",
    "corp",
    "corporation",
    "co",
    "company",
}

_SEPARATORS_RE = re.compile(r"[\|\u2022•·]+")
_MULTISPACE_RE = re.compile(r"\s+")
_NON_VENDOR_CHARS_RE = re.compile(r"[^A-Za-z0-9&.\- _]")
_REF_LIKE_RE = re.compile(r"\b(?:ref|rrn|utr|txnid|txn|id)[:\-]?\s*[A-Za-z0-9\-]{4,}\b", flags=re.IGNORECASE)
_LONG_NUMBER_RE = re.compile(r"\b\d{4,}\b")


_CANONICAL_MAP = {
    # Common Indian vendors / patterns
    "amazon pay": "Amazon",
    "amazonpay": "Amazon",
    "amazon": "Amazon",
    "amzn": "Amazon",
    "swiggy ltd": "Swiggy",
    "swiggy": "Swiggy",
    "zomato": "Zomato",
    "uber": "Uber",
    "ola": "Ola",
    "google pay": "Google Pay",
    "gpay": "Google Pay",
    "phonepe": "PhonePe",
    "paytm": "Paytm",
    "flipkart": "Flipkart",
}


def _tokenize(s: str) -> list[str]:
    s = _SEPARATORS_RE.sub(" ", s)
    s = _NON_VENDOR_CHARS_RE.sub(" ", s)
    s = _MULTISPACE_RE.sub(" ", s).strip()
    if not s:
        return []
    return s.split(" ")


def _drop_noise(tokens: Iterable[str]) -> list[str]:
    out: list[str] = []
    for t in tokens:
        tl = t.lower().strip()
        if not tl:
            continue
        if tl in _NOISE_TOKENS:
            continue
        if tl in _LEGAL_SUFFIXES:
            continue
        # Drop long numeric identifiers
        if _LONG_NUMBER_RE.fullmatch(tl):
            continue
        out.append(t)
    return out


def normalize_vendor(raw: str) -> str:
    """
    Normalize vendor names from noisy transaction narrations.
    - Removes payment rails/noise tokens (UPI/PAYMENT/TXN/REF/etc.)
    - Removes legal suffixes (LTD/PVT/etc.)
    - Removes reference-like substrings and long numeric ids
    - Canonicalizes a small set of common vendor aliases
    """
    s = (raw or "").strip()
    if not s:
        return "Unknown"

    s = _REF_LIKE_RE.sub(" ", s)
    s = _MULTISPACE_RE.sub(" ", s).strip()

    tokens = _tokenize(s)
    tokens = _drop_noise(tokens)
    if not tokens:
        return "Unknown"

    cleaned = " ".join(tokens)
    cleaned = _MULTISPACE_RE.sub(" ", cleaned).strip()
    if not cleaned:
        return "Unknown"

    key = cleaned.lower().replace(".", "").strip()
    key = _MULTISPACE_RE.sub(" ", key)
    if key in _CANONICAL_MAP:
        return _CANONICAL_MAP[key]

    # If it contains canonical keys as a substring, pick the longest match.
    best = None
    for k, v in _CANONICAL_MAP.items():
        if k in key:
            if best is None or len(k) > len(best[0]):
                best = (k, v)
    if best:
        return best[1]

    # Title-case while preserving acronyms like "DMART" -> "DMART"
    parts = []
    for t in cleaned.split(" "):
        if len(t) <= 1:
            continue
        if t.isupper() and len(t) <= 6:
            parts.append(t)
        else:
            parts.append(t[:1].upper() + t[1:].lower())
    return " ".join(parts) if parts else "Unknown"

