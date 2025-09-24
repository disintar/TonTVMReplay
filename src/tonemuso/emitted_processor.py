# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass

from loguru import logger
from tonpy import Address, Cell
from tonpy.autogen.block import Transaction

from tonemuso.emulation import TxStepEmulator, extract_message_info
from tonemuso.trace_models import EmuMessage, EmulatedNode, MessageMeta, TxRecord
from tonemuso.utils import hex_to_b64

# Local type aliases to avoid circular imports
BlockKey = Tuple[int, int, int, int]
TxHashHex = str
MsgHashB64 = str


@dataclass
class EmittedChildCandidate:
    emu_msg: EmuMessage
    match: bool
    expected_in_b64: Optional[MsgHashB64]
    child_tx_hash_hex: Optional[TxHashHex]
    original_order_idx: Optional[int] = None


class EmittedMessageProcessor:
    def __init__(self, runner: "TraceOrderedRunner") -> None:
        self.r = runner

    def _emulate_buffer_txs(self, block_key: BlockKey, txs: List[TxRecord], account_addr: Address, state: Cell) -> \
    Tuple[Cell, int]:
        """
        Emulate buffered transactions (not in original trace) that belong to the given account.
        Starts from the provided account state and returns the updated state and the count of
        buffer txs applied. Updates runner.account_states1 for the given account.
        """
        if not txs:
            return state, 0
        em, em2 = self.r._get_emulators(block_key)
        cnt = 0
        st = state
        for bt in txs:
            # Only apply buffered txs for the same account
            bt_tlb = Transaction().cell_unpack(bt.tx, True)
            bt_account_int = int(bt_tlb.account_addr, 2)
            bt_addr = Address(f"{block_key[0]}:{hex(bt_account_int).upper()[2:].zfill(64)}")
            assert bt_addr == account_addr
            # Run stepper (no out msgs extraction) using current state
            step_b = TxStepEmulator(block=self.r.blocks[block_key], loglevel=self.r.loglevel,
                                    color_schema=self.r.color_schema, em=em, account_state_em1=st, em2=em2,
                                    account_state_em2=st, use_boc_for_diff=self.r.use_boc_for_diff)
            _out_b, new_st1, _new_st2, _ = step_b.emulate(bt, extract_out_msgs=False)
            st = new_st1
            cnt += 1

        # Persist updated state for this account
        self.r.account_states1[block_key][account_addr] = st
        return st, cnt

    def emulate_internal_message_one_layer(self, block_key: BlockKey, msg: Union[Dict[str, Any], EmuMessage], now: int,
                                           lt: int) -> Tuple[Optional[Dict[str, Any]], List[EmittedChildCandidate]]:
        """
        Emulate a single internal message (new transaction) without recursing into its out messages.
        Returns:
          - node dict for the generated tx (or emulation_failed), never recursed
          - pre-classified candidates for its out messages (against empty expected set)
        """
        # Normalize to dict same as recursive version
        if not isinstance(msg, dict) and hasattr(msg, 'dest'):
            msg = {
                'dest': getattr(msg, 'dest', None),
                'cell': getattr(msg, 'cell', None),
                'msg_hash': getattr(msg, 'msg_hash_hex', None),
                'bodyhash': getattr(msg, 'body_hash_hex', None),
                'opcode': getattr(msg, 'opcode', None),
                'bounce': getattr(msg, 'bounce', None),
                'bounced': getattr(msg, 'bounced', None),
            }
        dest_addr: Address = msg['dest']
        self.r._ensure_account_state(block_key, dest_addr)
        em, _em2 = self.r._get_emulators(block_key)
        state1 = (self.r.account_states1[block_key].get(dest_addr)
                  or self.r.default_initial_state.get(block_key, {}).get(dest_addr)
                  or self.r._fetch_state_for_account(block_key, dest_addr))
        try:
            ok = em.emulate_transaction(state1, msg['cell'], now, lt)
            if ok:
                new_state = em.account.to_cell()
                self.r.account_states1[block_key][dest_addr] = new_state
                self.r.global_overrides[dest_addr] = new_state
                tx_cell = em.transaction.to_cell()
                generated_transaction_hash = tx_cell.get_hash().upper()
                node: Dict[str, Any] = {
                    'tx_hash': hex_to_b64(generated_transaction_hash),
                    'in_msg_hash': hex_to_b64(msg['msg_hash']) if 'msg_hash' in msg else None,
                    'in_msg_body_hash': hex_to_b64(msg.get('bodyhash')) if msg.get('bodyhash') else None,
                    'opcode': msg.get('opcode'),
                    'destination': dest_addr.raw,
                    'bounce': msg.get('bounce'),
                    'bounced': msg.get('bounced'),
                    'mode': 'new_transaction',
                    'children': [],
                    'emulation_order': self.r._next_emulation_order()
                }
                info = extract_message_info(tx_cell)
                emitted_raw = info.get('out_msgs', [])
                # For generated tx there is no original mapping; pass empty parent hash so all are mismatches
                candidates = self.classify_emitted('', emitted_raw)
                return node, candidates
            else:
                return ({
                    'tx_hash': None,
                    'in_msg_hash': hex_to_b64(msg['msg_hash']) if msg.get('msg_hash') else None,
                    'in_msg_body_hash': hex_to_b64(msg.get('bodyhash')) if msg.get('bodyhash') else None,
                    'opcode': msg.get('opcode'),
                    'destination': dest_addr.raw if hasattr(dest_addr, 'raw') else str(dest_addr),
                    'bounce': msg.get('bounce'),
                    'bounced': msg.get('bounced'),
                    'mode': 'emulation_failed',
                    'children': []
                }, [])
        except Exception:
            return ({
                'tx_hash': None,
                'in_msg_hash': hex_to_b64(msg['msg_hash']) if isinstance(msg, dict) and msg.get('msg_hash') else None,
                'in_msg_body_hash': hex_to_b64(msg.get('bodyhash')) if isinstance(msg, dict) and msg.get('bodyhash') else None,
                'opcode': (msg.get('opcode') if isinstance(msg, dict) else None),
                'destination': (dest_addr.raw if 'dest_addr' in locals() and hasattr(dest_addr, 'raw') else (str(dest_addr) if 'dest_addr' in locals() else None)),
                'bounce': (msg.get('bounce') if isinstance(msg, dict) else None),
                'bounced': (msg.get('bounced') if isinstance(msg, dict) else None),
                'mode': 'emulation_failed',
                'children': []
            }, [])

    def classify_emitted(self,
                         parent_transaction_hash: TxHashHex,
                         emitted_list: List[Dict[str, Any]]) -> List[EmittedChildCandidate]:
        """
        Pre-classify emitted messages against the original trace for a given parent.
        Matching is order-respecting using the parent's expected out list.
        For each emitted message, set match=True only when destination matches the expected child's destination (override);
        otherwise mark as mismatch (new tx).
        """
        candidates: List[EmittedChildCandidate] = []
        expected = self.r.child_map.get(parent_transaction_hash, [])
        links = self.r.child_link_map.get(parent_transaction_hash, {})
        i = 0
        for cm0 in emitted_list:
            cm = EmuMessage.from_dict(cm0)
            match = False
            exp_b64: Optional[MsgHashB64] = None
            child_hex: Optional[TxHashHex] = None
            if i < len(expected):
                exp_b64 = expected[i]
                try:
                    # Destination override check only
                    child_hex = links.get(exp_b64)
                    if isinstance(child_hex, str):
                        txd = self.r.toncenter_trace.transactions.get(hex_to_b64(child_hex))
                        expected_dest_raw = Address(txd.in_msg.destination_raw).raw if txd and txd.in_msg else None
                        emitted_dest_raw = cm.dest.raw if cm.dest is not None else None
                        if expected_dest_raw is not None and emitted_dest_raw == expected_dest_raw:
                            match = True
                except Exception:
                    pass
            candidates.append(EmittedChildCandidate(emu_msg=cm, match=match, expected_in_b64=exp_b64 if match else None,
                                                    child_tx_hash_hex=child_hex if match else None,
                                                    original_order_idx=(i if match else None)))
            if match:
                i += 1
        return candidates

    def process_emitted_children_with_override(self,
                                               parent_block_key: BlockKey,
                                               parent_transaction_hash: TxHashHex,
                                               parent_tx: TxRecord,
                                               emitted_list: List[EmittedChildCandidate],
                                               out: List[Dict[str, Any]],
                                               visited: Set[TxHashHex]) -> Tuple[
        List[Dict[str, Any]], List[Tuple[BlockKey, TxHashHex, TxRecord, List[EmittedChildCandidate], Dict[str, Any]]]]:
        """
        Process pre-classified emitted messages for a parent transaction (single layer, no recursion).
        For matched items (override), emulate the original child tx with override_in_msg and compare/color.
        For mismatches, emulate as new transactions (they may recurse internally as they are new).
        Returns:
          - nodes: list of built child nodes to append under current parent
          - next_contexts: list of tuples (gc_block_key, child_tx_hash_hex, child_tx, gc_candidates, gc_node_dict)
            to be processed at the next DFS layer by the caller.
        """
        nodes: List[Dict[str, Any]] = []
        next_contexts: List[Tuple[BlockKey, TxHashHex, TxRecord, List[EmittedChildCandidate], Dict[str, Any]]] = []
        expected = self.r.child_map.get(parent_transaction_hash, [])
        links = self.r.child_link_map.get(parent_transaction_hash, {})
        i = 0
        for cand in emitted_list:
            if cand.match and i < len(expected):
                expected_in_b64 = cand.expected_in_b64
                child_transaction_hash = cand.child_tx_hash_hex
                gc_idx = self.r.tx_index.get(child_transaction_hash)
                gc_block_key, gc_tx = gc_idx
                # Prepare account state
                gc_tlb = Transaction().cell_unpack(gc_tx.tx, True)
                gc_account_int = int(gc_tlb.account_addr, 2)
                gc_account_addr = Address(f"{gc_block_key[0]}:{hex(gc_account_int).upper()[2:].zfill(64)}")
                emu, emu2 = self.r._get_emulators(gc_block_key)
                # Determine BEFORE state for child and record source for diagnostics
                gc_state_source = None
                if child_transaction_hash in self.r.before_states:
                    gc_state = self.r.before_states[child_transaction_hash]
                    gc_state_source = 'before_states'
                elif gc_account_addr in self.r.account_states1[gc_block_key]:
                    gc_state = self.r.account_states1[gc_block_key][gc_account_addr]
                    gc_state_source = 'account_states1'
                else:
                    dflt2 = (self.r.default_initial_state.get(gc_block_key, {}) or {}).get(gc_account_addr)
                    if dflt2 is not None:
                        gc_state = dflt2
                        gc_state_source = 'default_initial_state'
                    else:
                        gc_state = self.r._fetch_state_for_account(gc_block_key, gc_account_addr)
                        gc_state_source = 'fetched_liteclient' if gc_state is not None else 'none'
                self.r.account_states1[gc_block_key][gc_account_addr] = gc_state
                # Pre-apply buffered txs (if any) to account state for this account in this block
                gc_state, buf_count = self._emulate_buffer_txs(
                    gc_block_key,
                    getattr(gc_tx, 'before_txs', []) or [],
                    gc_account_addr,
                    gc_state
                )
                # Emulate with override message via unified stepper (always compare/color)
                step_gc = TxStepEmulator(block=self.r.blocks[gc_block_key], loglevel=self.r.loglevel,
                                         color_schema=self.r.color_schema, em=emu, account_state_em1=gc_state,
                                         em2=emu2, account_state_em2=gc_state, use_boc_for_diff=self.r.use_boc_for_diff)
                tmp_out3, new_state_gc, _ns, gc_out_msgs = step_gc.emulate(
                    gc_tx,
                    override_in_msg=cand.emu_msg.cell,
                    extract_out_msgs=True
                )
                out.extend(tmp_out3)
                # Build node and obtain mode from it to avoid double summarize
                gc_node_obj = EmulatedNode(
                    tx_hash_hex=child_transaction_hash,
                    children=[],
                    tmp_out=tmp_out3,
                    in_msg=cand.emu_msg
                )
                # Annotate buffer emulation count
                gc_node_obj.buffer_emulated_count = int(buf_count)
                mode = gc_node_obj.mode_info or {}
                self.r.account_states1[gc_block_key][gc_account_addr] = new_state_gc
                if mode.get('mode') in ('new_transaction', 'error'):
                    self.r.global_overrides[gc_account_addr] = new_state_gc
                gc_node = gc_node_obj.to_dict()
                # Annotate diagnostics: where BEFORE state was taken from and which account it was

                gc_node['account_state_source'] = gc_state_source
                gc_node['account_address'] = str(gc_account_addr)
                # Assign emulation order for this matched child node
                gc_node['emulation_order'] = self.r._next_emulation_order()
                # Classify grandchildren for next layer (do not recurse here)
                gc_emitted_raw = (gc_out_msgs or {}).get('out_msgs', [])
                gc_candidates = self.classify_emitted(child_transaction_hash, gc_emitted_raw)
                # Append this node now; its children will be attached by the caller when processing next_contexts
                nodes.append(gc_node)
                # Track used original pair
                if isinstance(expected_in_b64, str):
                    self.r.used_original_pairs.add((parent_transaction_hash, expected_in_b64))
                # Queue next context for DFS
                next_contexts.append((gc_block_key, child_transaction_hash, gc_tx, gc_candidates, gc_node))
                i += 1
            else:
                # Mismatch: treat as new tx and emulate one layer; enqueue its out messages as next_context
                extra_node, gc_candidates = self.emulate_internal_message_one_layer(
                    parent_block_key, cand.emu_msg, parent_tx.now, parent_tx.lt
                )
                if extra_node is not None:
                    nodes.append(extra_node)
                    # Push next context to process its out messages in the same layer-by-layer DFS
                    next_contexts.append((parent_block_key, '', parent_tx, gc_candidates, extra_node))

        # Any remaining expected children are missed
        while i < len(expected):
            miss_b64 = expected[i]
            exp_meta = (self.r.message_meta.get(parent_transaction_hash, {}) or {}).get(miss_b64)
            dest2 = exp_meta.destination_raw if isinstance(exp_meta, MessageMeta) else None
            if isinstance(dest2, str):
                dest2 = Address(dest2).raw
            nodes.append({
                'tx_hash': None,
                'in_msg_hash': miss_b64,
                'in_msg_body_hash': (exp_meta.body_hash_b64 if isinstance(exp_meta, MessageMeta) else None),
                'opcode': (exp_meta.opcode if isinstance(exp_meta, MessageMeta) else None),
                'destination': dest2,
                'mode': 'missed_transaction',
                'original_data': {
                    'tx_hash': None,
                    'destination': dest2,
                    'opcode': (exp_meta.opcode if isinstance(exp_meta, MessageMeta) else None)
                },
                'children': []
            })
            i += 1
        return nodes, next_contexts
