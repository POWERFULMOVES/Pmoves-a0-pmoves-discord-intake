"""Discord drop intake: inbound message receiver, gate review, and NATS routing."""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Optional


NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT = os.environ.get("DISCORD_INTAKE_SUBJECT", "discord.intake.v1")

_DEFAULT_RULES: Dict[str, Any] = {
    "blocked_authors": [],
    "allowed_channels": [],
    "required_keywords": [],
    "min_content_length": 1,
}


def receive_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a Discord event payload and return a normalized intake record.

    Args:
        payload: Raw Discord message event dict (webhook or bot gateway).

    Returns:
        Normalized intake record: message_id, author, channel_id, content,
        timestamp, raw_length, valid.
    """
    content = payload.get("content") or payload.get("text", "")
    author = (payload.get("author") or {}).get("username") or payload.get("username", "unknown")
    channel_id = str(payload.get("channel_id") or payload.get("channel", ""))
    message_id = str(payload.get("id") or payload.get("message_id", ""))
    timestamp = payload.get("timestamp") or payload.get("created_at", "")

    return {
        "message_id": message_id,
        "author": author,
        "channel_id": channel_id,
        "content": content,
        "timestamp": timestamp,
        "raw_length": len(content),
        "valid": bool(content and author),
    }


def gate_review(
    intake: Dict[str, Any],
    rules: Optional[Dict[str, Any]] = None,
) -> bool:
    """Apply gate rules. Returns True if the message is approved for ingestion.

    Args:
        intake: Normalized intake record from receive_message().
        rules: Gate config keys: blocked_authors, allowed_channels,
               required_keywords, min_content_length.
    """
    r = {**_DEFAULT_RULES, **(rules or {})}
    if not intake.get("valid"):
        return False
    if intake["author"] in r["blocked_authors"]:
        return False
    if r["allowed_channels"] and intake["channel_id"] not in r["allowed_channels"]:
        return False
    if len(intake["content"]) < r["min_content_length"]:
        return False
    if r["required_keywords"]:
        content_lower = intake["content"].lower()
        if not any(kw.lower() in content_lower for kw in r["required_keywords"]):
            return False
    return True


async def _publish_async(intake: Dict[str, Any], nats_url: str, subject: str) -> bool:
    try:
        import nats as natspy
        nc = await natspy.connect(nats_url)
        await nc.publish(subject, json.dumps(intake).encode("utf-8"))
        await nc.drain()
        return True
    except Exception as exc:
        import sys
        sys.stderr.write(f"[discord_intake] NATS publish failed: {exc}\n")
        return False


def publish_to_nats(
    intake: Dict[str, Any],
    nats_url: str = "",
    subject: str = "",
) -> bool:
    """Forward an approved intake record to NATS (discord.intake.v1 by default)."""
    return asyncio.run(_publish_async(intake, nats_url or NATS_URL, subject or NATS_SUBJECT))


def ingest(
    payload: Dict[str, Any],
    rules: Optional[Dict[str, Any]] = None,
    nats_url: str = "",
    subject: str = "",
) -> Dict[str, Any]:
    """Full pipeline: receive -> gate -> publish. Returns result dict with approved/published."""
    intake = receive_message(payload)
    approved = gate_review(intake, rules)
    published = publish_to_nats(intake, nats_url, subject) if approved else False
    return {**intake, "approved": approved, "published": published}
