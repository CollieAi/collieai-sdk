"""Thin provider adapters that turn a provider's streaming response into the
text-delta factory ``protect_stream`` / ``protect_buffered`` expect.

Adapters duck-type the provider client, so importing them never requires the
provider SDK; install the matching extra (``collieai[openai]`` /
``collieai[anthropic]``) to get the SDK itself.

    from collieai.adapters import openai_factory      # or anthropic_factory
"""
from .anthropic import anthropic_factory, anthropic_text_deltas
from .openai import openai_factory, openai_text_deltas

__all__ = [
    "openai_factory",
    "openai_text_deltas",
    "anthropic_factory",
    "anthropic_text_deltas",
]
