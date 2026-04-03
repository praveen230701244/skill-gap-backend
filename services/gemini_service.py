import os
from typing import Optional

import requests


class GeminiService:
    """
    Gemini API wrapper for chatbot advice generation.
    Returns None on any failure so route fallback logic can handle gracefully.
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.base_url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-pro:generateContent"
        )
        self.url = f"{self.base_url}?key={self.api_key}" if self.api_key else ""

    def generate_advice(self, system_prompt: str, user_prompt: str) -> Optional[str]:
        if not self.api_key or not self.url:
            return None

        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": f"{system_prompt}\n\n{user_prompt}",
                        }
                    ]
                }
            ]
        }

        try:
            response = requests.post(self.url, json=payload, timeout=20)
            if response.status_code >= 400:
                return None
            data = response.json()
            content = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text")
            )
            if not content:
                return None
            text = str(content).strip()
            return text or None
        except Exception:
            return None

