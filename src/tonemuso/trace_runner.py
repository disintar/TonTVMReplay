# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import defaultdict, OrderedDict

from tonpy import Cell, LiteClient, begin_cell, BlockId, Address
from tonpy.tvm.not_native.emulator_extern import EmulatorExtern
from tonpy.autogen.block import Transaction, Block, BlockInfo
from loguru import logger

from tonemuso.emulation import init_emulators, emulate_tx_step, emulate_tx_step_with_in_msg
from tonemuso.utils import b64_to_hex, hex_to_b64
from tonemuso.diff import get_shard_account_diff, make_json_dumpable, get_colored_diff

# Type aliases for readability
BlockKey = Tuple[int, int, int, int]
TxHashHex = str
MsgHashB64 = str


class TraceOrderedRunner:
    """
    Emulates a trace strictly as a tree starting from the first transaction in
    toncenter's transactions_order, mutating per-account state along the way.

    Responsibilities:
    - Manage per-block emulator instances and per-account states (mutable).
    - Build child maps from toncenter (expected children in_msg hashes and links to child tx hashes).
    - Compare emitted out messages (order-respecting) to expected children and recurse:
      - follow expected children to their real transactions (possibly in other blocks),
      - emulate extra internal messages inline (within the same block).
    - Fetch unknown account states via LiteClient using the correct master block.
    """

    def __init__(self,
                 raw_chunks: Optional[List[Tuple[Dict[str, Any], Cell, List[Dict[str, Any]]]]],
                 config_override: Optional[Dict[str, Any]],
                 loglevel: int,
                 color_schema: Optional[Dict[str, Any]],
                 tx_order_hex_upper: Optional[List[str]] = None,
                 toncenter_tx_map: Optional[Dict[str, Any]] = None,
                 toncenter_tx_details: Optional[Dict[str, Any]] = None,
                 lcparams: Optional[Dict[str, Any]] = None,
                 preindexed: Optional[Dict[str, Any]] = None):
        # Indices and caches
        self.tx_index: Dict[TxHashHex, Tuple[BlockKey, Dict[str, Any]]] = OrderedDict()
        self.blocks: Dict[BlockKey, Dict[str, Any]] = OrderedDict()
        self.block_emulators: Dict[BlockKey, Tuple[EmulatorExtern, Optional[EmulatorExtern]]] = OrderedDict()
        self.account_states1: Dict[BlockKey, Dict[Address, Cell]] = defaultdict(OrderedDict)
        # Default initial account state per block per account (for lazy seeding)
        # Structure: { block_key: { Address: Cell } }
        self.default_initial_state: Dict[BlockKey, Dict[Address, Cell]] = defaultdict(OrderedDict)
        # Per-transaction BEFORE states captured in main.collect_raw emulation
        self.before_states: Dict[TxHashHex, Cell] = OrderedDict()

        # Config
        self.config_override = config_override
        self.loglevel = loglevel
        self.color_schema = color_schema
        self.tx_order_hex_upper = tx_order_hex_upper or []

        # Results and global state overrides
        self.failed_traces: List[Dict[str, Any]] = []
        self.global_overrides: Dict[Address, Cell] = OrderedDict()
        # Track which original expected children were actually used for comparison
        # Each entry is a tuple (parent_tx_hash_hex, expected_in_msg_b64)
        self.used_original_pairs: Set[Tuple[TxHashHex, MsgHashB64]] = set()

        # Store original tx details and build message metadata for quick lookup
        self.original_tx_details: Dict[str, Any] = OrderedDict(toncenter_tx_details or {})
        self._build_message_meta()

        # Child maps built from toncenter trace map (will be sorted by in_msg.created_lt)
        self.child_map: Dict[TxHashHex, List[MsgHashB64]] = OrderedDict()
        self.child_link_map: Dict[TxHashHex, Dict[MsgHashB64, TxHashHex]] = OrderedDict()
        self._build_child_maps(toncenter_tx_map)

        # Keep original trace root for reporting (transformed to required TX schema)
        self.original_trace_root: Optional[Dict[str, Any]] = None
        self.emulated_trace_root: Optional[Dict[str, Any]] = None
        if isinstance(toncenter_tx_map, dict) and 'tx_hash' in toncenter_tx_map:
            self.original_trace_root = self._transform_original_trace(toncenter_tx_map)

        # Build raw indices for quick access (from preindexed or raw chunks)
        if preindexed is not None:
            try:
                self.tx_index = OrderedDict(preindexed.get('tx_index') or {})
                self.blocks = OrderedDict(preindexed.get('blocks') or {})
                # Do not pass emulators
                self.account_states1 = defaultdict(OrderedDict)
                # Default initial state per block per account (for lazy seeding)
                self.default_initial_state = defaultdict(OrderedDict, preindexed.get('default_initial_state') or {})
                self.before_states = OrderedDict(preindexed.get('before_states') or {})
            except Exception as e:
                logger.error(f"Failed to apply preindexed data; falling back to raw indexing: {e}")
                self._index_raw_chunks(raw_chunks or [])
        else:
            self._index_raw_chunks(raw_chunks or [])

        # Optional LiteClient for on-demand account state fetch
        self.lc: Optional[LiteClient] = None
        self._init_liteclient(lcparams)

    # ---------- Setup helpers ----------
    def _record_failed(self, kind: str, **payload) -> None:
        # Legacy helper retained; in trace mode we no longer push granular events.
        # Only a single trace_tree_comparison entry is appended in run().
        return

    def _summarize_mode(self, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Reduce emulate_tx_step(...) out entries to a compact node annotation with mode and optional info.
        - If any non-success entry exists, take the first one and include its 'mode'.
        - Also forward 'diff', 'color_schema_log', 'account_color_schema_log', 'account_color_level',
          'unchanged_emulator_tx_hash' if present in that entry.
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
                return summary
        return {'mode': 'success'}

    def _build_message_meta(self) -> None:
        # Build mapping: parent transaction_hash (hex) -> { in_msg_b64 -> { opcode, destination, body_hash } }
        self.message_meta: Dict[TxHashHex, Dict[MsgHashB64, Dict[str, Any]]] = OrderedDict()
        try:
            for b64_tx, info in (self.original_tx_details or {}).items():
                parent_transaction_hash = b64_to_hex(b64_tx)
                out = info.get('out_msgs') or []
                if not out:
                    continue
                meta_map: Dict[MsgHashB64, Dict[str, Any]] = OrderedDict()
                for m in out:
                    in_b64 = m.get('hash')  # outgoing message hash in base64
                    if not isinstance(in_b64, str):
                        continue
                    meta_map[in_b64] = {
                        'opcode': m.get('opcode'),
                        'destination': m.get('destination'),
                        'body_hash': (m.get('message_content') or {}).get('hash'),
                        'bounce': m.get('bounce'),
                        'bounced': m.get('bounced')
                    }
                if meta_map:
                    self.message_meta[parent_transaction_hash] = meta_map
        except Exception as e:
            logger.warning(f"Failed to build message meta: {e}")

    def _transform_original_trace(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform toncenter trace node structure into required TX schema:
        TX = {tx_hash, in_msg_hash, in_msg_body_hash, opcode, destination, bounce, bounced, lt, children: List[TX]}
        in_msg_body_hash/opcode/destination/bounce/bounced are taken from original_tx_details[tx_hash].in_msg
        lt is taken directly from original_tx_details[tx_hash]['lt'] (as provided by toncenter)
        """
        tx_b64 = node.get('tx_hash')
        in_msg_b64 = node.get('in_msg_hash')
        opcode = None
        body_hash = None
        destination = None
        in_bounce = None
        in_bounced = None
        tx_lt = None
        if isinstance(tx_b64, str):
            tx_info = (self.original_tx_details or {}).get(tx_b64) or {}
            in_msg = tx_info.get('in_msg') or {}
            opcode = in_msg.get('opcode')
            body_hash = (in_msg.get('message_content') or {}).get('hash')
            destination = in_msg.get('destination')
            in_bounce = in_msg.get('bounce')
            in_bounced = in_msg.get('bounced')
            tx_lt = tx_info.get('lt')
            # Normalize to Address(...).raw format if available
            if isinstance(destination, str):
                destination = Address(destination).raw
        children_nodes = []
        for ch in (node.get('children') or []):
            if isinstance(ch, dict):
                children_nodes.append(self._transform_original_trace(ch))
        return {
            'tx_hash': tx_b64,
            'in_msg_hash': in_msg_b64,
            'in_msg_body_hash': body_hash,
            'opcode': opcode,
            'destination': destination,
            'bounce': in_bounce,
            'bounced': in_bounced,
            'lt': tx_lt,
            'children': children_nodes
        }

    def _build_child_maps(self, toncenter_tx_map: Optional[Dict[str, Any]]) -> None:
        if not toncenter_tx_map:
            return
        try:
            # If we were given a single trace tree node (has tx_hash and children), walk it recursively
            def walk(node: Dict[str, Any]):
                parent_transaction_hash = b64_to_hex(node.get('tx_hash')) if node.get('tx_hash') else None
                children = node.get('children') or []

                # Sort children by their in_msg.created_lt from original_tx_details to ensure deterministic order
                def child_created_lt(ch_node: Dict[str, Any]) -> int:
                    try:
                        tx_b64 = ch_node.get('tx_hash')
                        if isinstance(tx_b64, str):
                            tx_info = (self.original_tx_details or {}).get(tx_b64) or {}
                            in_msg = tx_info.get('in_msg') or {}
                            clt = in_msg.get('created_lt')
                            if isinstance(clt, str):
                                return int(clt)
                            if isinstance(clt, int):
                                return clt
                    except Exception:
                        pass
                    return 0

                try:
                    children_sorted = sorted([c for c in children if isinstance(c, dict)], key=child_created_lt)
                except Exception:
                    # Fallback to original order if sorting fails
                    children_sorted = [c for c in children if isinstance(c, dict)]

                child_b64s: List[MsgHashB64] = []
                link_map: Dict[MsgHashB64, TxHashHex] = OrderedDict()
                for ch in children_sorted:
                    in_b = ch.get('in_msg_hash')
                    tx_b64 = ch.get('tx_hash')
                    if isinstance(in_b, str):
                        child_b64s.append(in_b)
                        if isinstance(tx_b64, str):
                            link_map[in_b] = b64_to_hex(tx_b64)
                if parent_transaction_hash:
                    self.child_map[parent_transaction_hash] = child_b64s
                    self.child_link_map[parent_transaction_hash] = link_map
                for ch in children_sorted:
                    walk(ch)

            walk(toncenter_tx_map)
        except Exception as e:
            logger.error(f"Failed to build child map: {e}")

    def _index_raw_chunks(self, raw_chunks: List[Tuple[Dict[str, Any], Cell, List[Dict[str, Any]]]]) -> None:
        for block, initial_account_state, txs in raw_chunks:
            blk = block['block_id']
            block_key: BlockKey = (blk.id.workchain, blk.id.shard, blk.id.seqno, blk.root_hash)
            self.blocks[block_key] = block
            # Record default initial state per account in this block (for lazy seeding)
            # Note: raw_chunks provide initial_account_state corresponding to the account of txs in this chunk.
            # We will assign it per-account below when we parse txs.
            for tx in txs:
                tx_tlb = Transaction().cell_unpack(tx['tx'], True)
                account_address = int(tx_tlb.account_addr, 2)
                account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")
                # Seed default account state maps
                if account_addr not in self.default_initial_state[block_key]:
                    self.default_initial_state[block_key][account_addr] = initial_account_state
                if account_addr not in self.account_states1[block_key]:
                    self.account_states1[block_key][account_addr] = initial_account_state
                transaction_hash = tx['tx'].get_hash().upper()
                self.tx_index[transaction_hash] = (block_key, tx)
                # Capture BEFORE state provided by collect_raw if available
                if 'before_state_em1' in tx and tx['before_state_em1'] is not None:
                    self.before_states[transaction_hash] = tx['before_state_em1']

    def _init_liteclient(self, lcparams: Optional[Dict[str, Any]]) -> None:
        if not lcparams:
            return
        try:
            p = dict(lcparams)
            p['logprefix'] = 'tracerunner'
            self.lc = LiteClient(**p)
        except Exception as e:
            logger.error(f"Failed to init LiteClient in TraceOrderedRunner: {e}")

    # ---------- Emulator / state helpers ----------
    def _get_emulators(self, block_key: BlockKey) -> Tuple[EmulatorExtern, Optional[EmulatorExtern]]:
        if block_key not in self.block_emulators:
            em, _em2 = init_emulators(self.blocks[block_key], self.config_override)
            self.block_emulators[block_key] = (em, None)  # emulator2 is not used in trace mode
        return self.block_emulators[block_key]

    def _fetch_state_for_account(self, block_key: BlockKey, account_addr: Address) -> Optional[Cell]:
        if self.lc is None:
            return None
        block = self.blocks[block_key]
        try:
            wc = block['block_id'].id.workchain
            if wc == -1:
                state_block = block['block_id']
            else:
                blk_cell = self.lc.get_block(block['block_id'])
                blk = Block().cell_unpack(blk_cell)
                blk_info = BlockInfo().cell_unpack(blk.info, True)
                master_seqno = blk_info.master_ref.master.seq_no
                state_block = self.lc.lookup_block(BlockId(-1, 0x8000000000000000, master_seqno)).blk_id
            st = self.lc.get_account_state(account_addr, state_block)
            if not st.root.is_null():
                return begin_cell().store_ref(st.root) \
                    .store_uint(int(st.last_trans_hash, 16), 256) \
                    .store_uint(st.last_trans_lt, 64).end_cell()
            else:
                return begin_cell().store_ref(begin_cell().store_uint(0, 1).end_cell()) \
                    .store_uint(int(st.last_trans_hash, 16), 256) \
                    .store_uint(st.last_trans_lt, 64).end_cell()
        except Exception as e:
            logger.warning(f"Account state fetch failed for {str(account_addr)}: {e}")
            return None

    def _ensure_account_state(self, block_key: BlockKey, account_addr: Address) -> None:
        if account_addr in self.account_states1[block_key]:
            return
        st = self._fetch_state_for_account(block_key, account_addr)
        if st is not None:
            self.account_states1[block_key][account_addr] = st
        else:
            logger.warning(f"No state available for account {str(account_addr)} in block {block_key}")

    def _emulate_internal_message_recursive(self, block_key: BlockKey, msg: Dict[str, Any], now: int, lt: int) -> \
            Optional[Dict[str, Any]]:
        # Destination account as Address
        dest_addr: Address = msg['dest']
        self._ensure_account_state(block_key, dest_addr)
        em, _ = self._get_emulators(block_key)
        state1 = (self.account_states1[block_key].get(dest_addr)
                  or self.default_initial_state.get(block_key, {}).get(dest_addr)
                  or self._fetch_state_for_account(block_key, dest_addr))
        try:
            ok = em.emulate_transaction(state1, msg['cell'], now, lt)
            if ok:
                # Update state and global override
                new_state = em.account.to_cell()
                self.account_states1[block_key][dest_addr] = new_state
                self.global_overrides[dest_addr] = new_state
                # Build node for this generated transaction
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
                    'children': []
                }
                # Recurse into out messages of this generated tx
                from tonemuso.emulation import extract_message_info
                info = extract_message_info(tx_cell)
                children = info.get('out_msgs', [])
                if getattr(self, '_enqueue', None):
                    # Schedule children for next depth (extras as extras)
                    parent_hex = generated_transaction_hash
                    for idx, child_msg in enumerate(children):
                        self._enqueue(self._current_depth + 1,
                                      {'exact': False, 'override': False, 'extra': True, 'order_idx': idx},
                                      (lambda block_key_local=block_key, child_msg_local=child_msg, now_local=now, lt_local=lt:
                                          self._emulate_internal_message_recursive(block_key_local, child_msg_local, now_local, lt_local)),
                                      lambda res, n=node: n['children'].append(res) if res is not None else None,
                                      kind='internal_emul',
                                      parent_hex=parent_hex)
                else:
                    for child_msg in children:
                        child_node = self._emulate_internal_message_recursive(block_key, child_msg, now, lt)
                        if child_node is not None:
                            node['children'].append(child_node)
                return node
            else:
                self._record_failed('extra_emulation_failed', dest=str(dest_addr),
                                    reason='emulate_transaction returned False',
                                    emitted_message={
                                        'opcode': msg.get('opcode'),
                                        'destination': str(dest_addr),
                                        'body_hash': hex_to_b64(msg.get('bodyhash')) if msg.get('bodyhash') else None
                                    })
                return None
        except Exception as e:
            self._record_failed('extra_emulation_exception', dest=str(dest_addr), error=str(e),
                                emitted_message={
                                    'opcode': msg.get('opcode'),
                                    'destination': str(dest_addr),
                                    'body_hash': hex_to_b64(msg.get('bodyhash')) if msg.get('bodyhash') else None
                                })
            return None

    def _process_emitted_children_with_override(self,
                                                parent_block_key: BlockKey,
                                                parent_transaction_hash: TxHashHex,
                                                parent_tx: Dict[str, Any],
                                                emitted_list: List[Dict[str, Any]],
                                                out: List[Dict[str, Any]],
                                                visited: Set[TxHashHex]) -> List[Dict[str, Any]]:
        """
        Process a sequence of emitted internal messages against expected children for parent_hex.
        Applies exact in_msg_hash match first; if mismatch, tries position+destination override emulation; else treats as extra.
        Returns list of built child nodes to append.
        """
        nodes: List[Dict[str, Any]] = []
        expected = self.child_map.get(parent_transaction_hash, [])
        links = self.child_link_map.get(parent_transaction_hash, {})
        i = 0
        for cm in emitted_list:
            cm_b64 = hex_to_b64(cm['msg_hash'])
            cbody_b64 = hex_to_b64(cm.get('bodyhash')) if cm.get('bodyhash') else None
            cop = cm.get('opcode')
            if i < len(expected) and cm_b64 == expected[i]:
                child_transaction_hash = links.get(cm_b64)
                if child_transaction_hash:
                    self.used_original_pairs.add((parent_transaction_hash, expected[i]))
                    gc_dest = cm.get('dest').raw if cm.get('dest') is not None else None
                    if getattr(self, '_enqueue', None):
                        # schedule exact child tx for next depth
                        self._enqueue(self._current_depth + 1,
                                      {'exact': True, 'override': False, 'extra': False, 'order_idx': i},
                                      (lambda transaction_hash_local=child_transaction_hash, out_local=out, visited_local=visited, in_b64_local=cm_b64, body_b64_local=cbody_b64, opcode_local=cop, dest_local=gc_dest, bounce_local=cm.get('bounce'), bounced_local=cm.get('bounced'):
                                          self._process_tx(transaction_hash_local, out_local, visited_local, in_b64_local, body_b64_local, opcode_local, dest_local, bounce_local, bounced_local)),
                                      lambda res, lst=nodes: lst.append(res) if res is not None else None,
                                      kind='tx',
                                      parent_hex=parent_transaction_hash)
                    else:
                        gc_node = self._process_tx(child_transaction_hash, out, visited, cm_b64, cbody_b64, cop, gc_dest,
                                                   cm.get('bounce'),
                                                   cm.get('bounced'))
                        if gc_node is not None:
                            nodes.append(gc_node)
                else:
                    expected_meta = (self.message_meta.get(parent_transaction_hash, {}) or {}).get(cm_b64) or {}
                    dest = expected_meta.get('destination')
                    if isinstance(dest, str):
                        dest = Address(dest).raw
                    nodes.append({
                        'tx_hash': None,
                        'in_msg_hash': cm_b64,
                        'in_msg_body_hash': expected_meta.get('body_hash'),
                        'opcode': expected_meta.get('opcode'),
                        'destination': dest,
                        'mode': 'missed_transaction',
                        'original_data': {
                            'tx_hash': None,
                            'destination': dest,
                            'opcode': expected_meta.get('opcode')
                        },
                        'children': []
                    })
                i += 1
            else:
                # Try position+destination override for this expected position
                if i < len(expected):
                    expected_in_b64 = expected[i]
                    child_transaction_hash = links.get(expected_in_b64)
                    expected_dest_raw = None
                    if child_transaction_hash:
                        child_b64 = hex_to_b64(child_transaction_hash)
                        child_info = (self.original_tx_details or {}).get(child_b64) or {}
                        in_msg_info = child_info.get('in_msg') or {}
                        expected_dest = in_msg_info.get('destination')
                        if isinstance(expected_dest, str):
                            expected_dest_raw = Address(expected_dest).raw
                        else:
                            expected_dest_raw = expected_dest
                    emitted_dest_raw = cm.get('dest').raw if cm.get('dest') is not None else None
                    if child_transaction_hash and expected_dest_raw is not None and emitted_dest_raw == expected_dest_raw and 'cell' in cm:
                        if getattr(self, '_enqueue', None):
                            # schedule override emulation for next depth as a runnable closure
                            def run_override(parent_block_key_local=parent_block_key,
                                             child_transaction_hash_local=child_transaction_hash,
                                             cm_local=cm,
                                             cbody_b64_local=cbody_b64,
                                             cop_local=cop,
                                             emitted_dest_raw_local=emitted_dest_raw,
                                             out_local=out,
                                             visited_local=visited,
                                             order_idx_local=i):
                                gc_idx2 = self.tx_index.get(child_transaction_hash_local)
                                if gc_idx2 is None:
                                    return None
                                gc_block_key2, gc_tx2 = gc_idx2
                                gc_tlb2 = Transaction().cell_unpack(gc_tx2['tx'], True)
                                gc_account_int2 = int(gc_tlb2.account_addr, 2)
                                gc_account_addr2 = Address(f"{gc_block_key2[0]}:{hex(gc_account_int2).upper()[2:].zfill(64)}")
                                emu2, _ = self._get_emulators(gc_block_key2)
                                gc_state2 = (self.global_overrides.get(gc_account_addr2)
                                             or self.account_states1[gc_block_key2].get(gc_account_addr2)
                                             or self.default_initial_state.get(gc_block_key2, {}).get(gc_account_addr2)
                                             or self._fetch_state_for_account(gc_block_key2, gc_account_addr2))
                                self.account_states1[gc_block_key2][gc_account_addr2] = gc_state2
                                tmp_outX, new_state_gc2, _nsX, gc_out_msgs2 = emulate_tx_step_with_in_msg(
                                    self.blocks[gc_block_key2], gc_tx2, emu2, gc_state2, gc_tx2.get('after_state_em2', None), cm_local['cell'], self.loglevel,
                                    self.color_schema, True
                                )
                                out_local.extend(tmp_outX)
                                mode2 = self._summarize_mode(tmp_outX)
                                self.account_states1[gc_block_key2][gc_account_addr2] = new_state_gc2
                                if mode2.get('mode') in ('new_transaction', 'error'):
                                    self.global_overrides[gc_account_addr2] = new_state_gc2
                                node2 = {
                                    'tx_hash': hex_to_b64(child_transaction_hash_local),
                                    'in_msg_hash': hex_to_b64(cm_local['msg_hash']),
                                    'in_msg_body_hash': cbody_b64_local,
                                    'opcode': cop_local,
                                    'destination': emitted_dest_raw_local,
                                    'mode': mode2.get('mode'),
                                    **({'diff': mode2['diff']} if 'diff' in mode2 else {}),
                                    **({'color_schema_log': mode2['color_schema_log']} if 'color_schema_log' in mode2 else {}),
                                    **({'unchanged_emulator_tx_hash': mode2['unchanged_emulator_tx_hash']} if 'unchanged_emulator_tx_hash' in mode2 else {}),
                                    **({'account_emulator_tx_hash_match': mode2['account_emulator_tx_hash_match']} if 'account_emulator_tx_hash_match' in mode2 else {}),
                                    'children': []
                                }
                                # Schedule grandchildren comparison for next depth
                                self._enqueue(self._current_depth + 1,
                                              {'exact': True, 'override': False, 'extra': False, 'order_idx': order_idx_local},
                                              (lambda parent_block_key_local=gc_block_key2, parent_hex_local=b64_to_hex(node2['tx_hash']), parent_tx_local=gc_tx2, emitted_list_local=(gc_out_msgs2 or {}).get('out_msgs', []), out_local2=out_local, visited_local2=visited_local:
                                                  self._process_emitted_children_with_override(parent_block_key_local, parent_hex_local, parent_tx_local, emitted_list_local, out_local2, visited_local2)),
                                              lambda lst, n=node2: n['children'].extend(lst or []),
                                              kind='children_with_override',
                                              parent_hex=child_transaction_hash_local)
                                return node2
                            self._enqueue(self._current_depth + 1,
                                          {'exact': False, 'override': True, 'extra': False, 'order_idx': i},
                                          run_override,
                                          lambda res, lst=nodes: lst.append(res) if res is not None else None,
                                          kind='tx',
                                          parent_hex=parent_transaction_hash)
                            self.used_original_pairs.add((parent_transaction_hash, expected_in_b64))
                            i += 1
                            continue
                        else:
                            gc_idx = self.tx_index.get(child_transaction_hash)
                            if gc_idx is not None:
                                gc_block_key, gc_tx = gc_idx
                                # Prepare account state
                                gc_tlb = Transaction().cell_unpack(gc_tx['tx'], True)
                                gc_account_int = int(gc_tlb.account_addr, 2)
                                gc_account_addr = Address(f"{gc_block_key[0]}:{hex(gc_account_int).upper()[2:].zfill(64)}")
                                emu, _ = self._get_emulators(gc_block_key)
                                gc_state = (self.global_overrides.get(gc_account_addr)
                                           or self.account_states1[gc_block_key].get(gc_account_addr)
                                           or self.default_initial_state.get(gc_block_key, {}).get(gc_account_addr)
                                           or self._fetch_state_for_account(gc_block_key, gc_account_addr))
                                self.account_states1[gc_block_key][gc_account_addr] = gc_state
                                # Emulate with override message
                                tmp_out3, new_state_gc, _ns, gc_out_msgs = emulate_tx_step_with_in_msg(
                                    self.blocks[gc_block_key], gc_tx, emu, gc_state, gc_tx.get('after_state_em2', None), cm['cell'], self.loglevel,
                                    self.color_schema, True
                                )
                                out.extend(tmp_out3)
                                # Determine mode and update state; save to global_overrides only for new/error
                                mode = self._summarize_mode(tmp_out3)
                                self.account_states1[gc_block_key][gc_account_addr] = new_state_gc
                                if mode.get('mode') in ('new_transaction', 'error'):
                                    self.global_overrides[gc_account_addr] = new_state_gc
                                # Build node for this overridden child and process its children recursively
                                gc_node = {
                                    'tx_hash': hex_to_b64(child_transaction_hash),
                                    'in_msg_hash': cm_b64,
                                    'in_msg_body_hash': cbody_b64,
                                    'opcode': cop,
                                    'destination': emitted_dest_raw,
                                    'mode': mode.get('mode'),
                                    **({'diff': mode['diff']} if 'diff' in mode else {}),
                                    **({'color_schema_log': mode['color_schema_log']} if 'color_schema_log' in mode else {}),
                                    **({'unchanged_emulator_tx_hash': mode['unchanged_emulator_tx_hash']} if 'unchanged_emulator_tx_hash' in mode else {}),
                                    **({'account_emulator_tx_hash_match': mode['account_emulator_tx_hash_match']} if 'account_emulator_tx_hash_match' in mode else {}),
                                    'children': []
                                }
                                # Recurse for grandchildren using this same logic
                                gc_children = self._process_emitted_children_with_override(
                                    gc_block_key, child_transaction_hash, gc_tx, gc_out_msgs.get('out_msgs', []), out,
                                    visited
                                )
                                gc_node['children'].extend(gc_children)
                                nodes.append(gc_node)
                                self.used_original_pairs.add((parent_transaction_hash, expected_in_b64))
                                i += 1
                                continue
                # Else treat as extra (new) message
                if cm is not None and 'cell' in cm:
                    if getattr(self, '_enqueue', None):
                        self._enqueue(self._current_depth + 1,
                                      {'exact': False, 'override': False, 'extra': True, 'order_idx': i},
                                      (lambda block_key_local=parent_block_key, cm_local=cm, now_local=parent_tx['now'], lt_local=parent_tx['lt']:
                                          self._emulate_internal_message_recursive(block_key_local, cm_local, now_local, lt_local)),
                                      lambda res, lst=nodes: lst.append(res) if res is not None else None,
                                      kind='internal_emul',
                                      parent_hex=parent_transaction_hash)
                    else:
                        extra_node = self._emulate_internal_message_recursive(parent_block_key, cm, parent_tx['now'],
                                                                              parent_tx['lt'])
                        if extra_node is not None:
                            nodes.append(extra_node)
                else:
                    # No cell to emulate; skip adding a node for this extra message.
                    pass
        # Any remaining expected children are missed
        while i < len(expected):
            miss_b64 = expected[i]
            exp_meta = (self.message_meta.get(parent_transaction_hash, {}) or {}).get(miss_b64) or {}
            dest2 = exp_meta.get('destination')
            if isinstance(dest2, str):
                dest2 = Address(dest2).raw
            nodes.append({
                'tx_hash': None,
                'in_msg_hash': miss_b64,
                'in_msg_body_hash': exp_meta.get('body_hash'),
                'opcode': exp_meta.get('opcode'),
                'destination': dest2,
                'mode': 'missed_transaction',
                'original_data': {
                    'tx_hash': None,
                    'destination': dest2,
                    'opcode': exp_meta.get('opcode')
                },
                'children': []
            })
            i += 1
        logger.debug(f"children_with_override parent={parent_transaction_hash} produced_nodes={len(nodes)} i_end={i} expected_len={len(expected)}")
        return nodes

    # ---------- Traversal ----------
    def _process_tx(self, transaction_hash: TxHashHex,
                    out: List[Dict[str, Any]],
                    visited: Set[TxHashHex],
                    in_msg_b64: Optional[str] = None,
                    in_msg_body_b64: Optional[str] = None,
                    in_opcode: Optional[str] = None,
                    in_destination: Optional[str] = None,
                    in_bounce: Optional[bool] = None,
                    in_bounced: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        if transaction_hash in visited:
            return None
        visited.add(transaction_hash)
        idx = self.tx_index.get(transaction_hash)
        if idx is None:
            logger.warning(f"Transaction from order not found in collected data: {transaction_hash}")
            return None
        block_key, tx = idx
        tx_tlb = Transaction().cell_unpack(tx['tx'], True)
        account_address = int(tx_tlb.account_addr, 2)
        account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")

        em, _ = self._get_emulators(block_key)
        # Priority metter!!!
        state1 = (self.global_overrides.get(account_addr)  # if any other tx come to this account_addr in past
                  or self.before_states.get(transaction_hash)
                  or self.account_states1[block_key].get(account_addr)
                  or self.default_initial_state.get(block_key, {}).get(account_addr)
                  or self._fetch_state_for_account(block_key, account_addr))
        # Sync the stored account state to the chosen BEFORE state
        self.account_states1[block_key][account_addr] = state1

        tmp_out, new_state1, _new_state2, out_msgs = emulate_tx_step(
            self.blocks[block_key],
            tx,
            em,
            None,
            state1,
            tx.get('after_state_em2', None),
            self.loglevel,
            self.color_schema,
            True,
            True
        )

        out.extend(tmp_out)
        self.account_states1[block_key][account_addr] = new_state1

        # Compute account state diff vs AFTER state from second emulator (if available from collect_raw)
        account_diff_obj = None
        sa_diff = None
        try:
            after_state_em2 = tx.get('after_state_em2')
            if after_state_em2 is not None and new_state1 is not None:
                sa_diff = get_shard_account_diff(new_state1, after_state_em2)
                # Also compute whether unchanged emulator produced the same tx hash (if collected)
                try:
                    unchanged_hash = tx.get('unchanged_emulator_tx_hash')
                    expected_hash = tx['tx'].get_hash()
                    account_diff_obj = {
                        'data': make_json_dumpable(sa_diff.to_dict()),
                        'account_emulator_tx_hash_match': (unchanged_hash == expected_hash)
                    }
                except Exception:
                    account_diff_obj = {'data': make_json_dumpable(sa_diff.to_dict())}
        except Exception as e:
            logger.error(f"ACCOUNT DIFF ERROR for tx {transaction_hash}: {e}")

        # Build node for emulated tx
        mode_info = self._summarize_mode(tmp_out)
        node: Dict[str, Any] = {
            'tx_hash': hex_to_b64(transaction_hash),
            'in_msg_hash': in_msg_b64,
            'in_msg_body_hash': in_msg_body_b64,
            'opcode': in_opcode,
            'destination': in_destination,
            'bounce': in_bounce,
            'bounced': in_bounced,
            'mode': mode_info.get('mode'),
            **({'diff': mode_info['diff']} if 'diff' in mode_info else {}),
            **({'unchanged_emulator_tx_hash': mode_info['unchanged_emulator_tx_hash']} if 'unchanged_emulator_tx_hash' in mode_info else {}),
            **({'color_schema_log': mode_info['color_schema_log']} if 'color_schema_log' in mode_info else {}),
            **({'account_emulator_tx_hash_match': mode_info['account_emulator_tx_hash_match']} if 'account_emulator_tx_hash_match' in mode_info else {}),
            'children': []
        }

        expected_children_ordered = self.child_map.get(transaction_hash, [])
        link_map = self.child_link_map.get(transaction_hash, {})

        emitted_list = out_msgs.get('out_msgs', [])
        i = 0
        for m in emitted_list:
            mh_b64 = hex_to_b64(m['msg_hash'])
            body_b64 = hex_to_b64(m.get('bodyhash')) if m.get('bodyhash') else None
            opcode = m.get('opcode')
            if i < len(expected_children_ordered) and mh_b64 == expected_children_ordered[i]:
                child_transaction_hash = link_map.get(mh_b64)
                if child_transaction_hash:
                    self.used_original_pairs.add((transaction_hash, expected_children_ordered[i]))
                    child_dest = m.get('dest').raw if m.get('dest') is not None else None
                    if getattr(self, '_enqueue', None):
                        # schedule exact child for next depth
                        self._enqueue(self._current_depth + 1,
                                      {'exact': True, 'override': False, 'extra': False, 'order_idx': i},
                                      (lambda transaction_hash_local=child_transaction_hash, out_local=out, visited_local=visited, in_b64_local=mh_b64, body_b64_local=body_b64, opcode_local=opcode, dest_local=child_dest, bounce_local=m.get('bounce'), bounced_local=m.get('bounced'):
                                          self._process_tx(transaction_hash_local, out_local, visited_local, in_b64_local, body_b64_local, opcode_local, dest_local, bounce_local, bounced_local)),
                                      lambda res, n=node: n['children'].append(res) if res is not None else None,
                                      kind='tx',
                                      parent_hex=transaction_hash)
                    else:
                        child_node = self._process_tx(child_transaction_hash, out, visited, mh_b64, body_b64, opcode,
                                                      child_dest,
                                                      m.get('bounce'), m.get('bounced'))
                        if child_node is not None:
                            node['children'].append(child_node)
                else:
                    expected_meta = (self.message_meta.get(transaction_hash, {}) or {}).get(mh_b64) or {}
                    dest = expected_meta.get('destination')
                    if isinstance(dest, str):
                        dest = Address(dest).raw
                    missed_node = {
                        'tx_hash': None,
                        'in_msg_hash': mh_b64,
                        'in_msg_body_hash': expected_meta.get('body_hash'),
                        'opcode': expected_meta.get('opcode'),
                        'destination': dest,
                        'mode': 'missed_transaction',
                        'original_data': {
                            'tx_hash': None,
                            'destination': dest,
                            'opcode': expected_meta.get('opcode')
                        },
                        'children': []
                    }
                    node['children'].append(missed_node)
                i += 1
            else:
                # Try position+destination match to emulate the expected child using overridden in_msg
                if i < len(expected_children_ordered):
                    expected_in_b64 = expected_children_ordered[i]
                    child_transaction_hash = link_map.get(expected_in_b64)
                    expected_dest_raw = None
                    if child_transaction_hash:
                        # Fetch expected child's destination from toncenter tx details (in_msg.destination)
                        child_b64 = hex_to_b64(child_transaction_hash)
                        child_info = (self.original_tx_details or {}).get(child_b64) or {}
                        in_msg_info = child_info.get('in_msg') or {}
                        expected_dest = in_msg_info.get('destination')
                        if isinstance(expected_dest, str):
                            expected_dest_raw = Address(expected_dest).raw
                        else:
                            expected_dest_raw = expected_dest
                    # Compare destinations
                    emitted_dest_raw = m.get('dest').raw if m.get('dest') is not None else None
                    if child_transaction_hash and expected_dest_raw is not None and emitted_dest_raw == expected_dest_raw and 'cell' in m:
                        if getattr(self, '_enqueue', None):
                            # Schedule override emulation as a closure task for next depth
                            def run_override_tx(child_transaction_hash_local=child_transaction_hash,
                                                m_local=m,
                                                body_b64_local=body_b64,
                                                opcode_local=opcode,
                                                emitted_dest_raw_local=emitted_dest_raw,
                                                out_local=out,
                                                visited_local=visited,
                                                order_idx_local=i):
                                child_idx2 = self.tx_index.get(child_transaction_hash_local)
                                if child_idx2 is None:
                                    return None
                                child_block_key2, child_tx2 = child_idx2
                                child_tlb2 = Transaction().cell_unpack(child_tx2['tx'], True)
                                child_account_int2 = int(child_tlb2.account_addr, 2)
                                child_account_addr2 = Address(
                                    f"{child_block_key2[0]}:{hex(child_account_int2).upper()[2:].zfill(64)}")
                                em_child2, _ = self._get_emulators(child_block_key2)
                                child_state2 = (self.global_overrides.get(child_account_addr2)
                                                or self.account_states1[child_block_key2].get(child_account_addr2)
                                                or self.default_initial_state.get(child_block_key2, {}).get(child_account_addr2)
                                                or self._fetch_state_for_account(child_block_key2, child_account_addr2))
                                self.account_states1[child_block_key2][child_account_addr2] = child_state2
                                tmp_outY, new_state_child2, _nsY, child_out_msgs2 = emulate_tx_step_with_in_msg(
                                    self.blocks[child_block_key2], child_tx2, em_child2, child_state2, child_tx2.get('after_state_em2', None), m_local['cell'], self.loglevel,
                                    self.color_schema, True
                                )
                                out_local.extend(tmp_outY)
                                child_mode2 = self._summarize_mode(tmp_outY)
                                self.account_states1[child_block_key2][child_account_addr2] = new_state_child2
                                if child_mode2.get('mode') in ('new_transaction', 'error'):
                                    self.global_overrides[child_account_addr2] = new_state_child2
                                child_node2 = {
                                    'tx_hash': hex_to_b64(child_transaction_hash_local),
                                    'in_msg_hash': hex_to_b64(m_local['msg_hash']),
                                    'in_msg_body_hash': body_b64_local,
                                    'opcode': opcode_local,
                                    'destination': emitted_dest_raw_local,
                                    'bounce': m_local.get('bounce'),
                                    'bounced': m_local.get('bounced'),
                                    'mode': child_mode2.get('mode'),
                                    **({'diff': child_mode2['diff']} if 'diff' in child_mode2 else {}),
                                    **({'unchanged_emulator_tx_hash': child_mode2['unchanged_emulator_tx_hash']} if 'unchanged_emulator_tx_hash' in child_mode2 else {}),
                                    **({'color_schema_log': child_mode2['color_schema_log']} if 'color_schema_log' in child_mode2 else {}),
                                    **({'account_emulator_tx_hash_match': child_mode2['account_emulator_tx_hash_match']} if 'account_emulator_tx_hash_match' in child_mode2 else {}),
                                    'children': []
                                }
                                # Schedule grandchildren processing via _process_emitted_children_with_override for next depth
                                self._enqueue(self._current_depth + 1,
                                              {'exact': True, 'override': False, 'extra': False, 'order_idx': order_idx_local},
                                              (lambda parent_block_key_local=child_block_key2, parent_hex_local=b64_to_hex(child_node2['tx_hash']), parent_tx_local=child_tx2, emitted_list_local=(child_out_msgs2 or {}).get('out_msgs', []), out_local2=out_local, visited_local2=visited_local:
                                                  self._process_emitted_children_with_override(parent_block_key_local, parent_hex_local, parent_tx_local, emitted_list_local, out_local2, visited_local2)),
                                              lambda lst, n=child_node2: n['children'].extend(lst or []),
                                              kind='children_with_override',
                                              parent_hex=child_transaction_hash_local)
                                return child_node2
                            self._enqueue(self._current_depth + 1,
                                          {'exact': False, 'override': True, 'extra': False, 'order_idx': i},
                                          run_override_tx,
                                          lambda res, n=node: n['children'].append(res) if res is not None else None,
                                          kind='tx',
                                          parent_hex=transaction_hash)
                            self.used_original_pairs.add((transaction_hash, expected_in_b64))
                            i += 1
                            continue
                        else:
                            # Emulate the real child tx but override its in_msg with the emitted one
                            child_idx = self.tx_index.get(child_transaction_hash)
                            if child_idx is not None:
                                child_block_key, child_tx = child_idx
                                # Prepare child account state
                                child_tlb = Transaction().cell_unpack(child_tx['tx'], True)
                                child_account_int = int(child_tlb.account_addr, 2)
                                child_account_addr = Address(
                                    f"{child_block_key[0]}:{hex(child_account_int).upper()[2:].zfill(64)}")
                                em_child, _ = self._get_emulators(child_block_key)
                                child_state = (self.global_overrides.get(child_account_addr)
                                               or self.account_states1[child_block_key].get(child_account_addr)
                                               or self.default_initial_state.get(child_block_key, {}).get(child_account_addr)
                                               or self._fetch_state_for_account(child_block_key, child_account_addr))
                                self.account_states1[child_block_key][child_account_addr] = child_state
                                # Run override emulation with child's timing
                                tmp_out2, new_state_child, _ns2, child_out_msgs = emulate_tx_step_with_in_msg(
                                    self.blocks[child_block_key], child_tx, em_child, child_state, child_tx.get('after_state_em2', None), m['cell'], self.loglevel,
                                    self.color_schema, True
                                )
                                out.extend(tmp_out2)
                                # Determine mode and update states; save to global_overrides only for new/error
                                child_mode = self._summarize_mode(tmp_out2)
                                self.account_states1[child_block_key][child_account_addr] = new_state_child
                                if child_mode.get('mode') in ('new_transaction', 'error'):
                                    self.global_overrides[child_account_addr] = new_state_child
                                # Build child node and process its children inline using produced out msgs
                                child_node = {
                                    'tx_hash': hex_to_b64(child_transaction_hash),
                                    'in_msg_hash': mh_b64,
                                    'in_msg_body_hash': body_b64,
                                    'opcode': opcode,
                                    'destination': emitted_dest_raw,
                                    'bounce': m.get('bounce'),
                                    'bounced': m.get('bounced'),
                                    'mode': child_mode.get('mode'),
                                    **({'diff': child_mode['diff']} if 'diff' in child_mode else {}),
                                    **({'unchanged_emulator_tx_hash': child_mode['unchanged_emulator_tx_hash']} if 'unchanged_emulator_tx_hash' in child_mode else {}),
                                    **({'color_schema_log': child_mode['color_schema_log']} if 'color_schema_log' in child_mode else {}),
                                    **({'account_emulator_tx_hash_match': child_mode['account_emulator_tx_hash_match']} if 'account_emulator_tx_hash_match' in child_mode else {}),
                                    'children': []
                                }
                                # Process child's emitted messages against its expected children with override logic
                                child_nodes = self._process_emitted_children_with_override(
                                    child_block_key,
                                    child_transaction_hash,
                                    child_tx,
                                    child_out_msgs.get('out_msgs', []),
                                    out,
                                    visited
                                )
                                child_node['children'].extend(child_nodes)
                                node['children'].append(child_node)
                                # Mark this expected child (by position) as used for comparison
                                self.used_original_pairs.add((transaction_hash, expected_in_b64))
                                # We consumed one expected child position
                                i += 1
                                continue
                # Fallback to previous behavior: treat as extra
                if m is not None and 'cell' in m:
                    if getattr(self, '_enqueue', None):
                        self._enqueue(self._current_depth + 1,
                                      {'exact': False, 'override': False, 'extra': True, 'order_idx': i},
                                      (lambda block_key_local=block_key, m_local=m, now_local=tx['now'], lt_local=tx['lt']:
                                          self._emulate_internal_message_recursive(block_key_local, m_local, now_local, lt_local)),
                                      lambda res, n=node: n['children'].append(res) if res is not None else None,
                                      kind='internal_emul',
                                      parent_hex=transaction_hash)
                    else:
                        child_node = self._emulate_internal_message_recursive(block_key, m, tx['now'], tx['lt'])
                        if child_node is not None:
                            node['children'].append(child_node)
                else:
                    # No cell to emulate; skip adding a node for this extra message.
                    pass

        while i < len(expected_children_ordered):
            missing_b64 = expected_children_ordered[i]
            expected_meta = (self.message_meta.get(transaction_hash, {}) or {}).get(missing_b64) or {}
            dest3 = expected_meta.get('destination')
            if isinstance(dest3, str):
                dest3 = Address(dest3).raw
            original_child_transaction_hash = link_map.get(missing_b64)
            orig_child_b64 = hex_to_b64(original_child_transaction_hash) if original_child_transaction_hash else None
            missed_node3 = {
                'tx_hash': None,
                'in_msg_hash': missing_b64,
                'in_msg_body_hash': expected_meta.get('body_hash'),
                'opcode': expected_meta.get('opcode'),
                'destination': dest3,
                'mode': 'missed_transaction',
                'original_data': {
                    'tx_hash': orig_child_b64,
                    'destination': dest3,
                    'opcode': expected_meta.get('opcode')
                },
                'children': []
            }
            node['children'].append(missed_node3)
            i += 1
        logger.debug(f"_process_tx tx={transaction_hash} emitted={len(emitted_list)} expected={len(expected_children_ordered)} node_mode={node.get('mode')}")
        return node

    def _collect_in_hashes(self, node: Optional[Dict[str, Any]]) -> Set[str]:
        seen: Set[str] = set()

        def dfs(n):
            if not isinstance(n, dict):
                return
            ih = n.get('in_msg_hash')
            if isinstance(ih, str):
                seen.add(ih)
            for ch in n.get('children', []) or []:
                dfs(ch)

        dfs(node)
        return seen

    def _collect_not_presented(self, orig: Optional[Dict[str, Any]], present: Set[str]) -> List[Dict[str, Any]]:
        """
        Determine original transactions that were not used for comparison.
        We mark a child as 'used' if, during comparison, we actually processed its real transaction
        (exact in_msg match or position+destination override). Anything else is considered not presented.
        Note: 'present' is ignored; we rely on self.used_original_pairs.
        """
        missing: List[Dict[str, Any]] = []

        def walk(node: Optional[Dict[str, Any]]):
            if not isinstance(node, dict):
                return
            parent_b64 = node.get('tx_hash')
            parent_hex = None
            if isinstance(parent_b64, str):
                try:
                    parent_hex = b64_to_hex(parent_b64)
                except Exception:
                    parent_hex = None
            children = node.get('children', []) or []
            if parent_hex:
                for ch in children:
                    if not isinstance(ch, dict):
                        continue
                    ih = ch.get('in_msg_hash')
                    if isinstance(ih, str) and (parent_hex, ih) not in self.used_original_pairs:
                        missing.append({
                            'tx_hash': ch.get('tx_hash'),
                            'in_msg_hash': ih,
                            'in_msg_body_hash': ch.get('in_msg_body_hash'),
                            'opcode': ch.get('opcode'),
                            'destination': ch.get('destination'),
                            'bounce': ch.get('bounce'),
                            'bounced': ch.get('bounced'),
                            'children': []
                        })
            for ch in children:
                walk(ch)

        walk(orig)
        return missing

    def run(self, tx_order_hex_upper: List[str]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        order_list = tx_order_hex_upper or self.tx_order_hex_upper
        if not order_list:
            return out
        root_transaction_hash = order_list[0]
        root_in = self.original_trace_root.get('in_msg_hash') if self.original_trace_root else None
        # Root in_msg body/opcode/destination/bounce flags from original tx details
        root_b64 = hex_to_b64(root_transaction_hash)
        root_info = (self.original_tx_details or {}).get(root_b64) or {}
        root_in_msg = root_info.get('in_msg') or {}
        root_body = (root_in_msg.get('message_content') or {}).get('hash')
        root_opcode = root_in_msg.get('opcode')
        root_destination = root_in_msg.get('destination')
        root_bounce = root_in_msg.get('bounce')
        root_bounced = root_in_msg.get('bounced')
        if isinstance(root_destination, str):
            root_destination = Address(root_destination).raw

        # By-depth (level-order) run scheduler replacing DFS.
        # We will store curried calls to _process_tx, _process_emitted_children_with_override, and _emulate_internal_message_recursive
        # grouped by depth. For ordering within a depth, we sort by original trace order rules:
        # 1) exact in_msg_hash match first; 2) position+destination override; 3) extras last.
        visited: Set[str] = set()
        depth_map: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        emu_root_holder: List[Optional[Dict[str, Any]]] = [None]

        # Enqueue function available to inner methods
        def enqueue(depth: int, meta: Dict[str, Any], func, on_result, kind: str, parent_hex: Optional[str]):
            # logger.debug(f"ENQUEUE d={depth} kind={kind} parent={parent_hex} meta={meta}")
            item = {
                'meta': meta,
                'func': func,
                'on_result': on_result,
                'kind': kind,
                'parent_hex': parent_hex
            }
            # For children_with_override we must attach results after the next depth runs its tasks
            if kind == 'children_with_override':
                item['defer_attach_depth'] = depth + 1
            depth_map[depth].append(item)

        # enable by-depth mode for inner functions
        self._enqueue = enqueue
        self._current_depth = 0

        # Seed root _process_tx as depth 0 task
        self._enqueue(0,
                      {'exact': True, 'override': False, 'extra': False, 'order_idx': 0},
                      (lambda transaction_hash_local=root_transaction_hash, out_local=out, visited_local=visited, in_b64_local=root_in, body_b64_local=root_body, opcode_local=root_opcode, dest_local=root_destination, bounce_local=root_bounce, bounced_local=root_bounced:
                          self._process_tx(transaction_hash_local, out_local, visited_local, in_b64_local, body_b64_local, opcode_local, dest_local, bounce_local, bounced_local)),
                      lambda res: emu_root_holder.__setitem__(0, res),
                      kind='tx',
                      parent_hex=None)

        # Execute depth by depth
        cur_depth = 0
        deferred_by_depth: Dict[int, List[Tuple[Any, Any]]] = defaultdict(list)
        while cur_depth in depth_map:
            items = depth_map[cur_depth]
            # Sort to ensure: all items with original order_idx (from original trace) come first in that exact order,
            # then all new/extra txs. Within the 'new' group, keep previous priorities (exact, override, others).
            def sort_key(it):
                meta = it.get('meta') or {}
                has_pos = isinstance(meta.get('order_idx'), int)
                pos = meta.get('order_idx') if has_pos else 1_000_000
                # Group 0: items tied to original trace (have integer order_idx)
                group = 0 if has_pos else 1
                # Within group 1 (new transactions), preserve previous class priority
                if meta.get('exact'):
                    cls = 0
                elif meta.get('override'):
                    cls = 1
                else:
                    cls = 2
                # Extras are considered new by definition (group 1). Ensure they sort after non-extra within new.
                extra_flag = 1 if meta.get('extra') else 0
                # Final key: group first, then position (for group 0), then class and extra within group 1.
                # For group 0, cls/extra_flag don't matter as pos decides. For group 1, pos is large so cls/extras decide.
                return (group, pos, cls, extra_flag)
            items.sort(key=sort_key)

            # Prepare for next depth accumulation
            self._current_depth = cur_depth
            next_depth = cur_depth + 1
            # no local deferred; we keep them in deferred_by_depth to persist across depths
            # Execute all items at this depth
            for it in items:
                func = it.get('func')
                res = None
                if callable(func):
                    res = func()
                else:
                    # Treat precomputed objects (dict/list/None) as already computed result; log for diagnostics
                    res = func
                    logger.error(f"Non-callable scheduled func encountered at depth {self._current_depth}: kind={it.get('kind')} parent={it.get('parent_hex')} type={type(func).__name__}")
                cb = it.get('on_result')
                if cb:
                    # callbacks accept either (res) or (res, ...); support 1-arg lambdas used above
                    # logger.debug(f"CALLBACK d={self._current_depth} kind={it.get('kind')} parent={it.get('parent_hex')} res_type={type(res).__name__}")
                    # Defer attaching results from children_with_override until the next depth is executed
                    defer_to = it.get('defer_attach_depth')
                    if isinstance(defer_to, int):
                        deferred_by_depth[defer_to].append((cb, res))
                    else:
                        try:
                            cb(res)
                        except TypeError:
                            # Some callbacks capture additional positional args via default bindings
                            cb(res)
            # Now that all tasks at this depth executed, process any deferred attachments targeting this depth
            to_run = deferred_by_depth.pop(cur_depth, [])
            for cb_func, res_val in to_run:
                try:
                    cb_func(res_val)
                except TypeError:
                    cb_func(res_val)
            cur_depth = next_depth

        self.emulated_trace_root = emu_root_holder[0]
        # Prepare not_presented list by a second recursive pass
        present_hashes = self._collect_in_hashes(self.emulated_trace_root)
        not_presented = self._collect_not_presented(self.original_trace_root, present_hashes)

        # Record original and emulated trace (no diff) into failed_traces.json
        # Compute final status breakdown
        def _count_modes(node):
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

        counts = _count_modes(self.emulated_trace_root or {})
        # missed also includes original children not presented at all
        missed_total = counts.get('missed', 0) + len(not_presented)

        # Helper: normalize opcode to int if possible
        def _norm_op(v) -> Optional[int]:
            try:
                if v is None:
                    return None
                if isinstance(v, int):
                    return v
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s.startswith('0x'):
                        return int(s, 16)
                    return int(s)
            except Exception:
                return None
            return None

        # Check if extra new tx is allowed by color schema (diff_colored.json)
        # Return number of allowed "new_transaction" nodes (0 if not allowed)
        def _allowed_extra_new_count(root_node) -> int:
            try:
                schema = self.color_schema if isinstance(self.color_schema, dict) else {}
                trace_cfg = schema.get('trace') or {}
                allow = trace_cfg.get('allow_extra_tx') or []
                if not isinstance(allow, list) or not allow:
                    return 0
                # Build allow sets
                allow_plain: Set[int] = set()
                allow_bounced: Set[int] = set()
                for it in allow:
                    if not isinstance(it, dict):
                        continue
                    op_raw = it.get('op')
                    opn = _norm_op(op_raw)
                    if opn is None:
                        continue
                    allow_plain.add(opn)
                    if bool(it.get('with_bounced')):
                        allow_bounced.add(opn)

                # Respect global constraints: warnings/unsuccess/missed must still fail
                if counts.get('unsuccess', 0) != 0 or counts.get('warnings', 0) != 0:
                    return 0
                if missed_total != 0:
                    return 0

                # Collect all new nodes
                new_nodes: List[Dict[str, Any]] = []
                def dfs_collect(n):
                    if not isinstance(n, dict):
                        return
                    if n.get('mode') == 'new_transaction':
                        new_nodes.append(n)
                    for ch in (n.get('children') or []):
                        dfs_collect(ch)
                dfs_collect(root_node)

                if not new_nodes:
                    return 0
                # Only allow scenarios originating from a single extra root node (and at most one bounced child)
                if len(new_nodes) > 2:
                    return 0

                # Identify the top-most new node (one whose parent is not new). Build parent map first.
                parent_of: Dict[int, Optional[int]] = {}
                flat: List[Dict[str, Any]] = []
                def dfs_parent(n, parent_idx=None):
                    if not isinstance(n, dict):
                        return
                    idx = len(flat)
                    flat.append(n)
                    parent_of[idx] = parent_idx
                    for ch in (n.get('children') or []):
                        dfs_parent(ch, idx)
                dfs_parent(root_node)
                new_indices = [i for i, node in enumerate(flat) if node.get('mode') == 'new_transaction']
                if not new_indices:
                    return 0
                # Pick roots among new nodes
                new_roots = [i for i in new_indices if parent_of.get(i) is None or flat[parent_of[i]].get('mode') != 'new_transaction']
                if len(new_roots) != 1:
                    return 0
                root_new_idx = new_roots[0]
                root_new = flat[root_new_idx]
                op_int = _norm_op(root_new.get('opcode'))
                if op_int is None or op_int not in allow_plain:
                    return 0
                children = root_new.get('children') or []
                # Case A: no children
                if len(children) == 0:
                    # Ensure there are no other new nodes
                    return 1 if len(new_nodes) == 1 else 0
                # Case B: exactly one bounced child allowed if configured
                if len(children) == 1 and op_int in allow_bounced:
                    ch = children[0]
                    if isinstance(ch, dict) and ch.get('mode') == 'new_transaction' and bool(ch.get('bounced')) is True:
                        # Child must have no further children
                        if len(ch.get('children') or []) == 0 and len(new_nodes) == 2:
                            return 2
                return 0
            except Exception:
                return 0

        allowed_extra_applied = False
        allowed_extra_new = 0
        if self.emulated_trace_root is not None:
            allowed_extra_new = _allowed_extra_new_count(self.emulated_trace_root)
            allowed_extra_applied = allowed_extra_new > 0

        final_success = (
            (self.emulated_trace_root is not None)
            and counts.get('warnings', 0) == 0
            and counts.get('unsuccess', 0) == 0
            and (counts.get('new', 0) - allowed_extra_new) == 0
            and missed_total == 0
        )

        # Decide final status: success, warning (only warnings), or error (unsuccess/new/missed present)
        if final_success:
            final_status = 'success'
        else:
            only_warnings = (
                counts.get('unsuccess', 0) == 0 and
                (counts.get('new', 0) - allowed_extra_new) == 0 and
                missed_total == 0 and
                counts.get('warnings', 0) > 0
            )
            final_status = 'warning' if only_warnings else 'error'

        entry = {
            'type': 'trace_tree_comparison',
            'root_tx': hex_to_b64(root_transaction_hash),
            'original_trace': self.original_trace_root,
            'emulated_trace': self.emulated_trace_root,
            'not_presented': not_presented,
            'final_status': final_status
        }
        if allowed_extra_applied:
            entry['allowed_extra_applied'] = True
        if final_status != 'success':
            entry['final_status_detailed'] = {
                'success': counts.get('success', 0),
                'unsuccess': counts.get('unsuccess', 0),
                'warnings': counts.get('warnings', 0),
                'new': counts.get('new', 0),
                'missed': missed_total
            }
        self.failed_traces.append(entry)
        return out
