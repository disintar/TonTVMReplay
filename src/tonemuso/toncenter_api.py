# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Any, Dict, Optional, List
import requests


class ToncenterAPI:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: float = 30.0, session: Optional[requests.Session] = None):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.session = session or requests.Session()

    def _headers(self) -> Dict[str, str]:
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }
        if self.api_key:
            headers['X-API-Key'] = self.api_key
        return headers

    # Typed responses
    def _parse_trace(self, tr_obj: Dict[str, Any]) -> "TonTrace":
        from tonemuso.toncenter_models import TonTrace, TonTraceNode, TonTxDetails, TonMessageRef, BlockRef
        # transactions
        txs: Dict[str, TonTxDetails] = {}
        tx_map = tr_obj.get('transactions') or {}
        for tx_b64, info in tx_map.items():
            # in_msg
            in_src = info.get('in_msg') or {}
            in_msg = TonMessageRef(
                hash_b64=in_src.get('hash'),
                opcode=in_src.get('opcode'),
                destination_raw=in_src.get('destination'),
                body_hash=(in_src.get('message_content') or {}).get('hash'),
                bounce=in_src.get('bounce'),
                bounced=in_src.get('bounced'),
                created_lt=in_src.get('created_lt') if isinstance(in_src.get('created_lt'), int) else None,
            )
            # out_msgs list
            outs = []
            for m in (info.get('out_msgs') or []):
                outs.append(TonMessageRef(
                    hash_b64=m.get('hash'),
                    opcode=m.get('opcode'),
                    destination_raw=m.get('destination'),
                    body_hash=(m.get('message_content') or {}).get('hash'),
                    bounce=m.get('bounce'),
                    bounced=m.get('bounced'),
                    created_lt=m.get('created_lt') if isinstance(m.get('created_lt'), int) else None,
                ))
            bref = info.get('block_ref') or {}
            # shard comes as hex string, convert to int when possible
            shard_raw = bref.get('shard')
            try:
                shard_int = int(shard_raw, 16) if isinstance(shard_raw, str) else int(shard_raw)
            except Exception:
                shard_int = 0
            block_ref = None
            if bref:
                try:
                    block_ref = BlockRef(workchain=int(bref.get('workchain')), shard=shard_int, seqno=int(bref.get('seqno')))
                except Exception:
                    block_ref = None
            # Parse lt robustly: accept int or numeric string
            lt_raw = info.get('lt')
            try:
                if isinstance(lt_raw, int):
                    lt_val = lt_raw
                elif isinstance(lt_raw, str):
                    lt_val = int(lt_raw)
                else:
                    lt_val = None
            except Exception:
                lt_val = None
            txs[tx_b64] = TonTxDetails(
                tx_b64=tx_b64,
                lt=lt_val,
                in_msg=in_msg,
                out_msgs=outs,
                block_ref=block_ref,
            )
        # trace node
        def parse_node(n: Dict[str, Any]) -> TonTraceNode:
            return TonTraceNode(
                tx_hash_b64=n.get('tx_hash'),
                in_msg_hash_b64=n.get('in_msg_hash'),
                children=[parse_node(c) for c in (n.get('children') or []) if isinstance(c, dict)],
            )
        node = None
        if isinstance(tr_obj.get('trace'), dict):
            node = parse_node(tr_obj['trace'])
        order = tr_obj.get('transactions_order') or []
        return TonTrace(transactions_order_b64=list(order), transactions=txs, trace=node)

    def get_traces_by_hash_typed(self, tx_hash: Optional[str] = None, msg_hash: Optional[str] = None,
                                 include_actions: bool = False, limit: int = 10, offset: int = 0, sort: str = 'desc') -> List["TonTrace"]:
        if not tx_hash and not msg_hash:
            raise ValueError("Either tx_hash or msg_hash must be provided")
        url = f"{self.base_url}/traces"
        params: Dict[str, Any] = {
            'include_actions': 'true' if include_actions else 'false',
            'limit': int(limit),
            'offset': int(offset),
            'sort': sort,
        }
        if tx_hash and tx_hash.strip():
            params['tx_hash'] = tx_hash.strip()
        else:
            params['msg_hash'] = (msg_hash or '').strip()
        resp = self.session.get(url, params=params, headers=self._headers(), timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        traces = []
        for t in (data.get('traces') or []):
            try:
                traces.append(self._parse_trace(t))
            except Exception:
                continue
        return traces

    def get_traces_by_lt_range_typed(self, start_lt: int, end_lt: int, include_actions: bool = False,
                                     limit: int = 1000, offset: int = 0, sort: str = 'desc') -> List["TonTrace"]:
        url = f"{self.base_url}/traces"
        params: Dict[str, Any] = {
            'start_lt': int(start_lt),
            'end_lt': int(end_lt),
            'include_actions': 'true' if include_actions else 'false',
            'limit': int(limit),
            'offset': int(offset),
            'sort': sort,
        }
        resp = self.session.get(url, params=params, headers=self._headers(), timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        traces = []
        for t in (data.get('traces') or []):
            try:
                traces.append(self._parse_trace(t))
            except Exception:
                continue
        return traces
