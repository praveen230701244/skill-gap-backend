from typing import Any, Dict, Optional


class AzureOpenAIService:
    def __init__(
        self,
        api_key: str,
        endpoint: str,
        deployment: str,
        api_version: str = "2024-02-15-preview",
        timeout_seconds: float = 20.0,
    ):
        if not api_key or not endpoint or not deployment:
            raise ValueError("Missing Azure OpenAI configuration.")

        self.api_key = api_key
        self.endpoint = endpoint
        self.deployment = deployment
        self.api_version = api_version
        self.timeout_seconds = timeout_seconds

    def generate_advice(self, system_prompt: str, user_prompt: str) -> str:
        try:
            from openai import AzureOpenAI
        except Exception as e:
            raise RuntimeError("openai package is not installed or failed to import.") from e

        client = AzureOpenAI(
            azure_endpoint=self.endpoint,
            api_key=self.api_key,
            api_version=self.api_version,
            timeout=self.timeout_seconds,
        )

        try:
            completion = client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
                max_tokens=800,
            )
        except Exception as e:
            raise RuntimeError("Azure OpenAI request failed.") from e

        content = completion.choices[0].message.content or ""
        content = content.strip()

        # Remove common formatting artifacts.
        if content.startswith("```"):
            content = content.strip("`").strip()
        return content

