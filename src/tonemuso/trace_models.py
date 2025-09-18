# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List

from tonpy import Address, Cell
from tonemuso.utils import hex_to_b64


@dataclass
class TxRecord:
    """
    Transaction container produced by collect_raw. Replaces dict-based tx entries.
    Compatible with legacy code via __getitem__/__setitem__ for keys 'tx','lt','now','is_tock'
    and added fields populated during collection.

    New:
    - before_txs: for in-trace TXs, list of preceding TXs in the same block that are not in the trace
      and occur immediately before this one (reset after each in-trace TX).
    """
    tx: Cell
    lt: int
    now: int
    is_tock: bool
    before_state_em2: Optional[Cell] = None
    after_state_em2: Optional[Cell] = None
    unchanged_emulator_tx_hash: Optional[str] = None
    before_txs: List["TxRecord"] = field(default_factory=list)

    # Minimal mapping-like interface for backward compatibility
    def __getitem__(self, key: str):
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        setattr(self, key, value)


@dataclass
class MessageMeta:
    opcode: Optional[str] = None
    destination_raw: Optional[str] = None
    body_hash_b64: Optional[str] = None
    bounce: Optional[bool] = None
    bounced: Optional[bool] = None


@dataclass
class EmuMessage:
    """
    Represents an emitted internal message produced by emulation.
    Values are primarily hex for hashes to align with emulator outputs.
    """
    msg_hash_hex: str
    body_hash_hex: Optional[str]
    opcode: Optional[str]
    dest: Optional[Address]
    bounce: Optional[bool]
    bounced: Optional[bool]
    cell: Optional[Cell]
    created_lt: Optional[int]

    @staticmethod
    def from_dict(d: dict) -> "EmuMessage":
        # d structure comes from emulation.extract_message_info/emulate_tx_step
        return EmuMessage(
            msg_hash_hex=d.get('msg_hash'),
            body_hash_hex=d.get('bodyhash'),
            opcode=d.get('opcode'),
            dest=d.get('dest'),
            bounce=d.get('bounce'),
            bounced=d.get('bounced'),
            cell=d.get('cell'),
            created_lt=d.get('created_lt'),
        )


class EmulatedNode:
    """
    Unified representation of an emulated trace node used by TraceOrderedRunner.
    Provides a to_dict() method that yields the legacy JSON structure expected by
    downstream consumers and failed_traces.json.

    Enhancements:
    - Accepts tmp_out (emulation outputs) and internally summarizes mode/diff.
    - Accepts in_msg and derives bounce/bounced and related fields internally.
    - Optionally carries buffer_emulated_count for pre-emulated before_txs. 
    """

    def __init__(
        self,
        tx_hash_hex: Optional[str],
        children: List[Dict[str, Any]],
        tmp_out: List[Dict[str, Any]],
        in_msg: Any,
    ) -> None:
        # Basic fields
        self.tx_hash_hex = tx_hash_hex
        self.in_msg_hash_b64: Optional[str] = None
        self.in_msg_body_b64: Optional[str] = None
        self.opcode: Optional[str] = None
        self.destination_raw: Optional[str] = None
        self.bounce: Optional[bool] = None
        self.bounced: Optional[bool] = None

        # Extended fields (computed internally)
        self.mode: Optional[str] = None
        self.diff: Optional[Dict[str, Any]] = None
        self.color_schema_log: Optional[Any] = None
        self.unchanged_emulator_tx_hash: Optional[str] = None
        self.account_emulator_tx_hash_match: Optional[bool] = None
        self.children = list(children) if children else []
        self.mode_info: Optional[Dict[str, Any]] = None
        # New: count of pre-emulated buffer txs (set by caller when applicable)
        self.buffer_emulated_count: Optional[int] = None

        # Inputs for internal processing (always provided in current project)
        self.tmp_out = tmp_out
        self.in_msg = in_msg

        # Always compute mode summary from tmp_out
        self.mode_info = EmulatedNode.summarize_mode(self.tmp_out)

        # Derive fields from in_msg (EmuMessage or Toncenter model)
        # Bounce flags
        self.bounce = getattr(self.in_msg, 'bounce', None)
        self.bounced = getattr(self.in_msg, 'bounced', None)

        # In message hash (b64): prefer hash_b64; else convert msg_hash_hex
        ih_b64 = getattr(self.in_msg, 'hash_b64', None)
        if isinstance(ih_b64, str) and ih_b64:
            self.in_msg_hash_b64 = ih_b64
        else:
            ih_hex = getattr(self.in_msg, 'msg_hash_hex', None)
            if isinstance(ih_hex, str) and ih_hex:
                self.in_msg_hash_b64 = hex_to_b64(ih_hex)

        # Body hash (b64): prefer body_hash; else convert body_hash_hex
        ib_b64 = getattr(self.in_msg, 'body_hash', None)
        if isinstance(ib_b64, str) and ib_b64:
            self.in_msg_body_b64 = ib_b64
        else:
            ib_hex = getattr(self.in_msg, 'body_hash_hex', None)
            if isinstance(ib_hex, str) and ib_hex:
                self.in_msg_body_b64 = hex_to_b64(ib_hex)

        # Opcode
        self.opcode = getattr(self.in_msg, 'opcode', None)

        # Destination raw: prefer destination_raw; else dest (Address or str)
        dest_val = getattr(self.in_msg, 'destination_raw', None)
        if dest_val is None:
            dest_val = getattr(self.in_msg, 'dest', None)
        if hasattr(dest_val, 'raw'):
            self.destination_raw = dest_val.raw
        elif isinstance(dest_val, str):
            self.destination_raw = Address(dest_val).raw
        else:
            self.destination_raw = dest_val

    @staticmethod
    def summarize_mode(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Reduce emulate_tx_step(...) out entries to a compact node annotation with mode and optional info.
        - If any non-success entry exists, take the first one and include its 'mode'.
        - Also forward 'diff', 'color_schema_log', 'account_emulator_tx_hash_match', 'unchanged_emulator_tx_hash' if present.
        - Otherwise, return {'mode': 'success'}.
        """

        for e in entries or []:
            m = e.get('mode')
            if m and m != 'success':
                summary = {'mode': m}
                if 'diff' in e:
                    summary['diff'] = e['diff']
                if 'color_schema_log' in e:
                    summary['color_schema_log'] = e['color_schema_log']
                if 'unchanged_emulator_tx_hash' in e:
                    summary['unchanged_emulator_tx_hash'] = e['unchanged_emulator_tx_hash']
                if 'account_emulator_tx_hash_match' in e:
                    summary['account_emulator_tx_hash_match'] = e['account_emulator_tx_hash_match']
                if 'account_code_hash' in e:
                    summary['account_code_hash'] = e['account_code_hash']
                # Map legacy fail_reason to error_reason and forward if present
                if 'error_reason' in e and e.get('error_reason'):
                    summary['error_reason'] = e.get('error_reason')
                elif 'fail_reason' in e and e.get('fail_reason'):
                    summary['error_reason'] = e.get('fail_reason')
                return summary

        return {'mode': 'success'}

    def to_dict(self) -> Dict[str, Any]:
        # Prefer mode_info dict if provided; fall back to explicit fields
        minfo = self.mode_info or {}
        mode_val = self.mode if self.mode is not None else minfo.get('mode')
        diff_val = self.diff if self.diff is not None else minfo.get('diff')
        csl_val = self.color_schema_log if self.color_schema_log is not None else minfo.get('color_schema_log')
        uhash_val = self.unchanged_emulator_tx_hash if self.unchanged_emulator_tx_hash is not None else minfo.get('unchanged_emulator_tx_hash')
        acct_match_val = self.account_emulator_tx_hash_match if self.account_emulator_tx_hash_match is not None else minfo.get('account_emulator_tx_hash_match')
        acct_code_hash_val = minfo.get('account_code_hash')

        node: Dict[str, Any] = {
            'tx_hash': (hex_to_b64(self.tx_hash_hex) if isinstance(self.tx_hash_hex, str) else None),
            'in_msg_hash': self.in_msg_hash_b64,
            'in_msg_body_hash': self.in_msg_body_b64,
            'opcode': self.opcode,
            'destination': self.destination_raw,
            'bounce': self.bounce,
            'bounced': self.bounced,
            'mode': mode_val,
            'children': list(self.children) if self.children else [],
        }
        if diff_val is not None:
            node['diff'] = diff_val
        if uhash_val is not None:
            node['unchanged_emulator_tx_hash'] = uhash_val
        if csl_val is not None:
            node['color_schema_log'] = csl_val
        if acct_match_val is not None:
            node['account_emulator_tx_hash_match'] = acct_match_val
        if acct_code_hash_val is not None:
            node['account_code_hash'] = acct_code_hash_val
        # Forward error_reason if present in mode_info (or explicitly set later)
        err_reason = (self.mode_info or {}).get('error_reason')
        if err_reason:
            node['error_reason'] = err_reason
        if isinstance(self.buffer_emulated_count, int):
            node['buffer_emulated_count'] = self.buffer_emulated_count
        return node
