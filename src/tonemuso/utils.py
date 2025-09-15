# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
import base64
from typing import Optional, Dict, Any


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


def count_modes_tree(node: Dict[str, Any]) -> Dict[str, int]:
    """
    Count modes in a nested emulated_trace tree structure.
    Returns a dict with keys: success, warnings, unsuccess, new, missed
    """
    from collections import Counter
    cnt = Counter()

    def dfs(n):
        if not isinstance(n, dict):
            return
        mode = n.get('mode')
        if mode == 'success':
            cnt['success'] += 1
        elif mode == 'warning':
            cnt['warnings'] += 1
        elif mode == 'error':
            cnt['unsuccess'] += 1
        elif mode == 'new_transaction':
            cnt['new'] += 1
        elif mode == 'missed_transaction':
            cnt['missed'] += 1
        for ch in (n.get('children') or []):
            dfs(ch)

    dfs(node)
    return cnt
