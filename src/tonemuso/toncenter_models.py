# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
from tonpy import Address


@dataclass
class BlockRef:
    workchain: int
    shard: int  # already int (convert from hex if needed at parsing time)
    seqno: int


@dataclass
class TonMessageRef:
    hash_b64: Optional[str] = None
    opcode: Optional[str] = None
    destination_raw: Optional[str] = None
    body_hash: Optional[str] = None
    bounce: Optional[bool] = None
    bounced: Optional[bool] = None
    created_lt: Optional[int] = None

    def to_legacy_dict(self) -> Dict:
        return {
            'hash': self.hash_b64,
            'opcode': self.opcode,
            'destination': self.destination_raw,
            'message_content': {'hash': self.body_hash} if self.body_hash is not None else {},
            'bounce': self.bounce,
            'bounced': self.bounced,
            'created_lt': self.created_lt,
        }


@dataclass
class TonTxDetails:
    tx_b64: str
    lt: Optional[int] = None
    in_msg: TonMessageRef = field(default_factory=TonMessageRef)
    out_msgs: List[TonMessageRef] = field(default_factory=list)
    block_ref: Optional[BlockRef] = None

    def to_legacy_dict(self) -> Dict:
        return {
            'lt': self.lt,
            'in_msg': self.in_msg.to_legacy_dict() if self.in_msg is not None else {},
            'out_msgs': [m.to_legacy_dict() for m in self.out_msgs] if self.out_msgs is not None else [],
        }


@dataclass
class TonTraceNode:
    tx_hash_b64: Optional[str] = None
    in_msg_hash_b64: Optional[str] = None
    children: List["TonTraceNode"] = field(default_factory=list)

    def node_to_dict(self) -> Dict:
        return {
            'tx_hash': self.tx_hash_b64,
            'in_msg_hash': self.in_msg_hash_b64,
            'children': [c.node_to_dict() for c in self.children],
        }

    def to_tx_schema(self, tx_details: Dict[str, "TonTxDetails"]) -> Dict:
        """
        Transform this node into the legacy TX schema used by the runner.
        TX = {tx_hash, in_msg_hash, in_msg_body_hash, opcode, destination, bounce, bounced, lt, children}
        Values are sourced from the in_msg of the corresponding TonTxDetails in tx_details.
        """
        tx_b64 = self.tx_hash_b64
        in_msg_b64 = self.in_msg_hash_b64
        opcode = None
        body_hash = None
        destination = None
        in_bounce = None
        in_bounced = None
        tx_lt = None
        if isinstance(tx_b64, str):
            txd = tx_details.get(tx_b64)
            if txd is not None:
                tx_lt = txd.lt
                if txd.in_msg is not None:
                    opcode = txd.in_msg.opcode
                    body_hash = txd.in_msg.body_hash
                    destination = txd.in_msg.destination_raw
                    in_bounce = txd.in_msg.bounce
                    in_bounced = txd.in_msg.bounced
                    # Normalize to Address(...).raw format if available
                    try:
                        if isinstance(destination, str):
                            destination = Address(destination).raw
                    except Exception:
                        pass
        children_nodes = [c.to_tx_schema(tx_details) for c in (self.children or [])]
        return {
            'tx_hash': tx_b64,
            'in_msg_hash': in_msg_b64,
            'in_msg_body_hash': body_hash,
            'opcode': opcode,
            'destination': destination,
            'bounce': in_bounce,
            'bounced': in_bounced,
            'lt': tx_lt,
            'children': children_nodes,
        }


@dataclass
class TonTrace:
    transactions_order_b64: List[str] = field(default_factory=list)
    transactions: Dict[str, TonTxDetails] = field(default_factory=dict)  # key: tx_b64
    trace: Optional[TonTraceNode] = None

    def to_dict(self) -> Dict:
        # Utility for debugging/serialization if ever needed
        return {
            'transactions_order': list(self.transactions_order_b64),
            'transactions': {k: asdict(v) for k, v in self.transactions.items()},
            'trace': (self.trace.node_to_dict() if self.trace is not None else None),
        }

    def build_original_tx_details_dict(self) -> Dict[str, Dict]:
        # Builds the legacy mapping {tx_b64: {lt, in_msg: {...}, out_msgs: [...]}}
        return {tx_b64: txd.to_legacy_dict() for tx_b64, txd in self.transactions.items()}

    def to_tx_schema_root(self) -> Optional[Dict]:
        """
        Build the legacy TX schema dict for the root node using this trace's transactions.
        """
        if self.trace is None:
            return None
        return self.trace.to_tx_schema(self.transactions)
