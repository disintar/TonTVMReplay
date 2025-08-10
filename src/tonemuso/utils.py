# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
import base64
from typing import Optional


def b64_to_hex(s: str, uppercase: bool = True) -> str:
    """
    Convert base64 string to hex string.
    If conversion fails, return the original string uppercased if uppercase=True.
    """
    try:
        h = base64.b64decode(s).hex()
        return h.upper() if uppercase else h
    except Exception:
        return s.upper() if uppercase else s


def hex_to_b64(s: str) -> str:
    """
    Convert hex string to base64 string.
    If conversion fails, return the original string.
    """
    try:
        return base64.b64encode(bytes.fromhex(s)).decode('ascii')
    except Exception:
        return s
