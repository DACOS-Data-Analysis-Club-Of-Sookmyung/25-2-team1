# src/llm/client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import requests


@dataclass
class OpenAICompatClient:
    """
    OpenAI-compatible Chat Completions 클라이언트 (로컬 vLLM/Kanana 등)

    generate.py에서 기대하는 인터페이스:
      client.chat(system: str, user: str, temperature: float, max_tokens: int) -> str
    """
    base_url: str
    api_key: str
    model: str
    timeout: int = 120

    def __post_init__(self) -> None:
        self.base_url = (self.base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url is required (e.g., http://localhost:8000/v1)")
        if not self.model:
            raise ValueError("model is required (e.g., kakaocorp/kanana-1.5-8b-instruct-2505)")

    def chat(
        self,
        system: str,
        user: str,
        temperature: float = 0.2,
        max_tokens: int = 1600,
    ) -> str:
        url = f"{self.base_url}/chat/completions"

        headers = {"Content-Type": "application/json"}
        # 로컬 vLLM은 보통 키가 필요 없지만, 호환성 위해 지원
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system or ""},
                {"role": "user", "content": user or ""},
            ],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }

        try:
            r = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            r.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"LLM request failed: {e}") from e

        data = r.json()

        # OpenAI 호환 응답 형태: choices[0].message.content
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            # 디버깅 도움용(응답 형태가 다를 때)
            raise RuntimeError(f"Unexpected LLM response schema: {data}")
