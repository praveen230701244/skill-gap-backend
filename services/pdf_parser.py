import re
from datetime import datetime
from typing import List, Dict, Any

AMOUNT_PATTERN = re.compile(r"(\d+(?:,\d{3})*(?:\.\d{2})?)")


def parse_pdf_bytes(file_bytes: bytes) -> List[Dict[str, Any]]:
    import fitz

    doc = fitz.open(stream=file_bytes, filetype="pdf")

    lines = []
    for page in doc:
        text = page.get_text()
        lines.extend([l.strip() for l in text.split("\n") if l.strip()])

    expenses = []

    for line in lines:
        l = line.lower()

        # ❌ Skip unwanted lines
        if any(x in l for x in ["total", "subtotal", "tax", "invoice"]):
            continue

        matches = AMOUNT_PATTERN.findall(line)
        if not matches:
            continue

        try:
            amount = float(matches[-1].replace(",", ""))
        except:
            continue

        if amount <= 0 or amount > 50000:
            continue

        expenses.append({
            "amount": amount,
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "vendor": line,
            "category": None
        })

    # Remove duplicates
    seen = set()
    final = []
    for e in expenses:
        key = (e["amount"], e["vendor"])
        if key not in seen:
            seen.add(key)
            final.append(e)

    return final