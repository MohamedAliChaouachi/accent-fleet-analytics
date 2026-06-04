"""Amazon Bedrock provider.

Supports Bedrock models via boto3 SDK. The OpenAI-compatible providers 
can't talk directly to Bedrock, so this provider uses the native AWS SDK.

Requires AWS credentials (via environment variables or IAM role):
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - AWS_REGION (default: us-east-1)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from app.ai.config import AISettings
from app.ai.providers.base import BaseLLMProvider, LLMProviderError, LLMResponse

if TYPE_CHECKING:  # pragma: no cover
    from botocore.client import BaseClient


class BedrockProvider(BaseLLMProvider):
    name = "bedrock"

    def __init__(self, settings: AISettings) -> None:
        try:
            import boto3
        except ImportError as e:  # pragma: no cover
            raise LLMProviderError(
                "AI_PROVIDER=bedrock but the `boto3` package is not "
                "installed. Add `boto3>=1.34.0` to requirements.txt."
            ) from e
        
        # Check for AWS credentials
        import os
        if not os.environ.get("AWS_ACCESS_KEY_ID") and not os.environ.get("AWS_PROFILE"):
            raise LLMProviderError(
                "AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
                "environment variables, or use an IAM role."
            )
        
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=settings.aws_region,
        )
        self.model = settings.bedrock_model

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return self._chat(system_prompt, user_prompt, temperature=0.0)

    def summarize_result(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return self._chat(system_prompt, user_prompt, temperature=0.2)

    def _chat(self, system_prompt: str, user_prompt: str, *, temperature: float) -> LLMResponse:
        # Bedrock Converse API format (works with most models including GLM)
        try:
            response = self._client.converse(
                modelId=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [{"text": user_prompt}],
                    }
                ],
                system=[{"text": system_prompt}],
                inferenceConfig={
                    "temperature": temperature,
                    "maxTokens": 2048,
                },
            )
        except Exception as e:  # noqa: BLE001
            raise LLMProviderError(f"bedrock converse call failed: {e}") from e

        # Extract text from response
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])
        
        text_parts: list[str] = []
        for block in content_blocks:
            if "text" in block:
                text_parts.append(block["text"])
        
        joined = "".join(text_parts).strip()
        if not joined:
            raise LLMProviderError("bedrock returned no text content")
        
        # Get actual model from response
        model_id = response.get("modelId", self.model)
        
        return LLMResponse(text=joined, model=model_id)
