from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression


DEFAULT_CATEGORY_RULES: Dict[str, List[str]] = {
    "Food & Drinks": ["starbucks", "mc donald", "mcdonald", "burger", "pizza", "dominos", "swiggy", "zomato", "coffee", "restaurant", "cafe", "uber eats", "grubhub"],
    "Transport": ["uber", "lyft", "taxi", "metro", "train", "fuel", "shell", "bp", "chevron", "petrol", "gas station"],
    "Shopping": ["amazon", "walmart", "target", "ikea", "best buy", "shopping", "store"],
    "Subscriptions": ["netflix", "spotify", "hulu", "prime video", "icloud", "google one", "dropbox", "zoom", "office 365"],
    "Travel": ["airbnb", "hotel", "marriott", "hilton", "booking.com", "expedia", "airlines", "flight", "uber travel"],
    "Utilities": ["electric", "water", "utility", "internet", "comcast", "verizon", "att", "vodafone"],
    "Software": ["microsoft", "google", "aws", "gcp", "adobe", "figma", "github", "gitlab"],
    "Health": ["pharmacy", "walgreens", "cvs", "doctor", "hospital", "dental", "pharma"],
    "Housing": ["rent", "landlord", "mortgage"],
}


def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(str(s).lower().split())


@dataclass
class AutoCategorizer:
    """
    ML-first categorizer:
    - Trains a TF-IDF + Logistic Regression classifier from labeled historical expenses.
    - Falls back to keyword rules when confidence is low or model is unavailable.
    """

    rules: Dict[str, List[str]]
    confidence_threshold: float = 0.55
    min_training_rows: int = 25
    _vectorizer: Optional[TfidfVectorizer] = field(default=None, init=False, repr=False)
    _classifier: Optional[LogisticRegression] = field(default=None, init=False, repr=False)

    @staticmethod
    def default() -> "AutoCategorizer":
        return AutoCategorizer(rules=DEFAULT_CATEGORY_RULES)

    def _rule_categorize(self, vendor: Optional[str]) -> str:
        v = _normalize_text(vendor)
        if not v:
            return "Uncategorized"
        for cat, keywords in self.rules.items():
            if any(kw in v for kw in keywords):
                return cat
        return "Uncategorized"

    def _can_train(self, expenses: List[Dict[str, Any]]) -> bool:
        labeled = [
            e
            for e in expenses
            if str(e.get("category") or "").strip()
            and str(e.get("category")).strip().lower() != "uncategorized"
            and str(e.get("vendor") or "").strip()
        ]
        if len(labeled) < self.min_training_rows:
            return False
        labels = {str(e["category"]).strip() for e in labeled}
        return len(labels) >= 2

    def train_from_expenses(self, expenses: List[Dict[str, Any]]) -> bool:
        if not self._can_train(expenses):
            self._vectorizer = None
            self._classifier = None
            return False

        labeled = [
            e
            for e in expenses
            if str(e.get("category") or "").strip()
            and str(e.get("vendor") or "").strip()
            and str(e.get("category")).strip().lower() != "uncategorized"
        ]
        X_text = [_normalize_text(str(e.get("vendor") or "")) for e in labeled]
        y = [str(e["category"]).strip() for e in labeled]

        try:
            vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_features=2000)
            X = vectorizer.fit_transform(X_text)
            classifier = LogisticRegression(max_iter=300, class_weight="balanced")
            classifier.fit(X, y)
        except Exception:
            self._vectorizer = None
            self._classifier = None
            return False

        self._vectorizer = vectorizer
        self._classifier = classifier
        return True

    def predict_category(self, vendor: Optional[str]) -> Optional[Dict[str, Any]]:
        if self._vectorizer is None or self._classifier is None:
            return None
        text = _normalize_text(vendor)
        if not text:
            return None
        try:
            X = self._vectorizer.transform([text])
            probs = self._classifier.predict_proba(X)[0]
            idx = int(np.argmax(probs))
            return {
                "category": str(self._classifier.classes_[idx]),
                "confidence": float(probs[idx]),
            }
        except Exception:
            return None

    def categorize(
        self,
        amount: float,
        category: Optional[str],
        vendor: Optional[str],
        historical_expenses: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        del amount  # reserved for future richer models
        if category and str(category).strip():
            return str(category).strip()

        if historical_expenses is not None:
            self.train_from_expenses(historical_expenses)

        pred = self.predict_category(vendor=vendor)
        if pred and float(pred["confidence"]) >= self.confidence_threshold:
            return str(pred["category"])

        return self._rule_categorize(vendor=vendor)

