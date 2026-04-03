import re
from datetime import datetime
from typing import List, Dict, Any

AMOUNT_PATTERN = re.compile(r"(\d+(?:,\d{3})*(?:\.\d{2})?)")


def parse_pdf_bytes(file_bytes: bytes) -> List[Dict[str, Any]]:
    try:
        import fitz
    except Exception as e:
        raise RuntimeError("PyMuPDF not installed. Run: pip install pymupdf") from e

    doc = fitz.open(stream=file_bytes, filetype="pdf")

    lines = []
    for page in doc:
        text = page.get_text()
        lines.extend([l.strip() for l in text.split("\n") if l.strip()])

    expenses = []

    for line in lines:
        l = line.lower()

        # ✅ smarter filtering
        if "total" in l and len(l) < 25:
            continue
        if "tax" in l:
            continue
        if "invoice" in l:
            continue

        matches = AMOUNT_PATTERN.findall(line)
        if not matches:
            continue

        try:
            amount = float(matches[-1].replace(",", ""))
        except Exception as e:
            print("PARSE ERROR:", e)
            continue

        if amount <= 0 or amount > 50000:
            continue

        expenses.append({
            "amount": amount,
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "vendor": line,
            "category": None
        })

    # ✅ remove duplicates
    seen = set()
    final = []
    for e in expenses:
        key = (e["amount"], e["vendor"])
        if key not in seen:
            seen.add(key)
            final.append(e)

    if not final:
        print("⚠️ No items extracted from PDF")

    return final