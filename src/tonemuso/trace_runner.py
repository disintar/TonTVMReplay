# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import defaultdict

from tonpy import Cell, LiteClient, begin_cell, BlockId, Address
from tonpy.tvm.not_native.emulator_extern import EmulatorExtern
from tonpy.autogen.block import Transaction, Block, BlockInfo
from loguru import logger

from tonemuso.emulation import init_emulators, emulate_tx_step, emulate_tx_step_with_in_msg
from tonemuso.utils import b64_to_hex, hex_to_b64

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
                 raw_chunks: List[Tuple[Dict[str, Any], Cell, List[Dict[str, Any]]]],
                 config_override: Optional[Dict[str, Any]],
                 loglevel: int,
                 color_schema: Optional[Dict[str, Any]],
                 tx_order_hex_upper: Optional[List[str]] = None,
                 toncenter_tx_map: Optional[Dict[str, Any]] = None,
                 toncenter_tx_details: Optional[Dict[str, Any]] = None,
                 lcparams: Optional[Dict[str, Any]] = None):
        # Indices and caches
        self.tx_index: Dict[TxHashHex, Tuple[BlockKey, Dict[str, Any]]] = {}
        self.blocks: Dict[BlockKey, Dict[str, Any]] = {}
        self.block_emulators: Dict[BlockKey, Tuple[EmulatorExtern, Optional[EmulatorExtern]]] = {}
        self.account_states1: Dict[BlockKey, Dict[Address, Cell]] = defaultdict(dict)

        # Config
        self.config_override = config_override
        self.loglevel = loglevel
        self.color_schema = color_schema
        self.tx_order_hex_upper = tx_order_hex_upper or []

        # Results and global state overrides
        self.failed_traces: List[Dict[str, Any]] = []
        self.global_overrides: Dict[Address, Cell] = {}

        # Child maps built from toncenter trace map
        self.child_map: Dict[TxHashHex, List[MsgHashB64]] = {}
        self.child_link_map: Dict[TxHashHex, Dict[MsgHashB64, TxHashHex]] = {}
        self._build_child_maps(toncenter_tx_map)
        # Keep original trace root for reporting (transformed to required TX schema)
        self.original_trace_root: Optional[Dict[str, Any]] = None
        self.emulated_trace_root: Optional[Dict[str, Any]] = None
        # Store original tx details and build message metadata for quick lookup
        self.original_tx_details: Dict[str, Any] = toncenter_tx_details or {}
        self._build_message_meta()
        if isinstance(toncenter_tx_map, dict) and 'tx_hash' in toncenter_tx_map:
            self.original_trace_root = self._transform_original_trace(toncenter_tx_map)

        # Build raw indices for quick access
        self._index_raw_chunks(raw_chunks)

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
        Reduce emulate_tx_step(...) out entries to a compact node annotation with mode and optional diff.
        - If any non-success entry exists, take the first one and include its 'mode' and 'diff' (if present).
        - Otherwise, return {'mode': 'success'}.
        """
        for e in entries or []:
            m = e.get('mode')
            if m and m != 'success':
                summary = {'mode': m}
                if 'diff' in e:
                    summary['diff'] = e['diff']
                return summary
        return {'mode': 'success'}

    def _build_message_meta(self) -> None:
        # Build mapping: parent_tx_hex -> { in_msg_b64 -> { opcode, destination, body_hash } }
        self.message_meta: Dict[TxHashHex, Dict[MsgHashB64, Dict[str, Any]]] = {}
        try:
            for b64_tx, info in (self.original_tx_details or {}).items():
                parent_hex = b64_to_hex(b64_tx)
                out = info.get('out_msgs') or []
                if not out:
                    continue
                meta_map: Dict[MsgHashB64, Dict[str, Any]] = {}
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
                    self.message_meta[parent_hex] = meta_map
        except Exception as e:
            logger.warning(f"Failed to build message meta: {e}")

    def _transform_original_trace(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform toncenter trace node structure into required TX schema:
        TX = {tx_hash, in_msg_hash, in_msg_body_hash, opcode, destination, bounce, bounced, children: List[TX]}
        in_msg_body_hash/opcode/destination/bounce/bounced are taken from original_tx_details[tx_hash].in_msg
        """
        tx_b64 = node.get('tx_hash')
        in_msg_b64 = node.get('in_msg_hash')
        opcode = None
        body_hash = None
        destination = None
        in_bounce = None
        in_bounced = None
        if isinstance(tx_b64, str):
            tx_info = (self.original_tx_details or {}).get(tx_b64) or {}
            in_msg = tx_info.get('in_msg') or {}
            opcode = in_msg.get('opcode')
            body_hash = (in_msg.get('message_content') or {}).get('hash')
            destination = in_msg.get('destination')
            in_bounce = in_msg.get('bounce')
            in_bounced = in_msg.get('bounced')
            # Normalize to Address(...).raw format if available
            try:
                if isinstance(destination, str):
                    destination = Address(destination).raw
            except Exception:
                pass
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
            'children': children_nodes
        }

    def _build_child_maps(self, toncenter_tx_map: Optional[Dict[str, Any]]) -> None:
        if not toncenter_tx_map:
            return
        try:
            # If we were given a single trace tree node (has tx_hash and children), walk it recursively
            def walk(node: Dict[str, Any]):
                parent_hex = b64_to_hex(node.get('tx_hash')) if node.get('tx_hash') else None
                children = node.get('children') or []
                child_b64s: List[MsgHashB64] = []
                link_map: Dict[MsgHashB64, TxHashHex] = {}
                for ch in children:
                    if not isinstance(ch, dict):
                        continue
                    in_b = ch.get('in_msg_hash')
                    tx_b64 = ch.get('tx_hash')
                    if isinstance(in_b, str):
                        child_b64s.append(in_b)
                        if isinstance(tx_b64, str):
                            link_map[in_b] = b64_to_hex(tx_b64)
                if parent_hex:
                    self.child_map[parent_hex] = child_b64s
                    self.child_link_map[parent_hex] = link_map
                for ch in children:
                    if isinstance(ch, dict):
                        walk(ch)
            walk(toncenter_tx_map)
        except Exception as e:
            logger.error(f"Failed to build child map: {e}")

    def _index_raw_chunks(self, raw_chunks: List[Tuple[Dict[str, Any], Cell, List[Dict[str, Any]]]]) -> None:
        for block, initial_account_state, txs in raw_chunks:
            blk = block['block_id']
            block_key: BlockKey = (blk.id.workchain, blk.id.shard, blk.id.seqno, blk.root_hash)
            self.blocks[block_key] = block
            for tx in txs:
                tx_tlb = Transaction().cell_unpack(tx['tx'], True)
                account_address = int(tx_tlb.account_addr, 2)
                account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")
                if account_addr not in self.account_states1[block_key]:
                    self.account_states1[block_key][account_addr] = initial_account_state
                h = tx['tx'].get_hash().upper()
                self.tx_index[h] = (block_key, tx)

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

    def _emulate_internal_message_recursive(self, block_key: BlockKey, msg: Dict[str, Any], now: int, lt: int) -> Optional[Dict[str, Any]]:
        # Destination account as Address
        dest_addr: Address = msg['dest']
        self._ensure_account_state(block_key, dest_addr)
        em, _ = self._get_emulators(block_key)
        state1 = self.account_states1[block_key][dest_addr]
        try:
            ok = em.emulate_transaction(state1, msg['cell'], now, lt)
            if ok:
                # Update state and global override
                new_state = em.account.to_cell()
                self.account_states1[block_key][dest_addr] = new_state
                self.global_overrides[dest_addr] = new_state
                # Build node for this generated transaction
                tx_cell = em.transaction.to_cell()
                tx_hex = tx_cell.get_hash().upper()
                node: Dict[str, Any] = {
                    'tx_hash': hex_to_b64(tx_hex),
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
                for child_msg in info.get('out_msgs', []):
                    child_node = self._emulate_internal_message_recursive(block_key, child_msg, now, lt)
                    if child_node is not None:
                        node['children'].append(child_node)
                return node
            else:
                self._record_failed('extra_emulation_failed', dest=str(dest_addr), reason='emulate_transaction returned False',
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
                                                parent_hex: TxHashHex,
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
        expected = self.child_map.get(parent_hex, [])
        links = self.child_link_map.get(parent_hex, {})
        i = 0
        for cm in emitted_list:
            cm_b64 = hex_to_b64(cm['msg_hash'])
            cbody_b64 = hex_to_b64(cm.get('bodyhash')) if cm.get('bodyhash') else None
            cop = cm.get('opcode')
            if i < len(expected) and cm_b64 == expected[i]:
                gc_hex = links.get(cm_b64)
                if gc_hex:
                    gc_dest = cm.get('dest').raw if cm.get('dest') is not None else None
                    gc_node = self._process_tx(gc_hex, out, visited, cm_b64, cbody_b64, cop, gc_dest, cm.get('bounce'), cm.get('bounced'))
                    if gc_node is not None:
                        nodes.append(gc_node)
                else:
                    expected_meta = (self.message_meta.get(parent_hex, {}) or {}).get(cm_b64) or {}
                    dest = expected_meta.get('destination')
                    try:
                        if isinstance(dest, str):
                            dest = Address(dest).raw
                    except Exception:
                        pass
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
                    gc_hex = links.get(expected_in_b64)
                    expected_dest_raw = None
                    if gc_hex:
                        child_b64 = hex_to_b64(gc_hex)
                        child_info = (self.original_tx_details or {}).get(child_b64) or {}
                        in_msg_info = child_info.get('in_msg') or {}
                        expected_dest = in_msg_info.get('destination')
                        try:
                            if isinstance(expected_dest, str):
                                expected_dest_raw = Address(expected_dest).raw
                        except Exception:
                            expected_dest_raw = expected_dest
                    emitted_dest_raw = cm.get('dest').raw if cm.get('dest') is not None else None
                    if gc_hex and expected_dest_raw is not None and emitted_dest_raw == expected_dest_raw and 'cell' in cm:
                        gc_idx = self.tx_index.get(gc_hex)
                        if gc_idx is not None:
                            gc_block_key, gc_tx = gc_idx
                            # Prepare account state
                            gc_tlb = Transaction().cell_unpack(gc_tx['tx'], True)
                            gc_account_int = int(gc_tlb.account_addr, 2)
                            gc_account_addr = Address(f"{gc_block_key[0]}:{hex(gc_account_int).upper()[2:].zfill(64)}")
                            emu, _ = self._get_emulators(gc_block_key)
                            gc_state = self.global_overrides.get(gc_account_addr) or self.account_states1[gc_block_key][gc_account_addr]
                            self.account_states1[gc_block_key][gc_account_addr] = gc_state
                            # Emulate with override message
                            tmp_out3, new_state_gc, _ns, gc_out_msgs = emulate_tx_step_with_in_msg(
                                self.blocks[gc_block_key], gc_tx, emu, gc_state, cm['cell'], self.loglevel, self.color_schema, True
                            )
                            out.extend(tmp_out3)
                            # Update state and global overrides
                            self.account_states1[gc_block_key][gc_account_addr] = new_state_gc
                            self.global_overrides[gc_account_addr] = new_state_gc
                            # Build node for this overridden child and process its children recursively
                            mode = self._summarize_mode(tmp_out3)
                            gc_node = {
                                'tx_hash': hex_to_b64(gc_hex),
                                'in_msg_hash': cm_b64,
                                'in_msg_body_hash': cbody_b64,
                                'opcode': cop,
                                'destination': emitted_dest_raw,
                                'mode': mode.get('mode'),
                                **({'diff': mode['diff']} if 'diff' in mode else {}),
                                'children': []
                            }
                            # Recurse for grandchildren using this same logic
                            gc_children = self._process_emitted_children_with_override(
                                gc_block_key, gc_hex, gc_tx, gc_out_msgs.get('out_msgs', []), out, visited
                            )
                            gc_node['children'].extend(gc_children)
                            nodes.append(gc_node)
                            i += 1
                            continue
                # Else treat as extra (new) message
                if cm is not None and 'cell' in cm:
                    extra_node = self._emulate_internal_message_recursive(parent_block_key, cm, parent_tx['now'], parent_tx['lt'])
                    if extra_node is not None:
                        nodes.append(extra_node)
                else:
                    # No cell to emulate; skip adding a node for this extra message.
                    pass
        # Any remaining expected children are missed
        while i < len(expected):
            miss_b64 = expected[i]
            exp_meta = (self.message_meta.get(parent_hex, {}) or {}).get(miss_b64) or {}
            dest2 = exp_meta.get('destination')
            try:
                if isinstance(dest2, str):
                    dest2 = Address(dest2).raw
            except Exception:
                pass
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
        return nodes

    # ---------- Traversal ----------
    def _process_tx(self, h: TxHashHex, out: List[Dict[str, Any]], visited: Set[TxHashHex], in_msg_b64: Optional[str] = None, in_msg_body_b64: Optional[str] = None, in_opcode: Optional[str] = None, in_destination: Optional[str] = None, in_bounce: Optional[bool] = None, in_bounced: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        if h in visited:
            return None
        visited.add(h)
        idx = self.tx_index.get(h)
        if idx is None:
            logger.warning(f"Transaction from order not found in collected data: {h}")
            return None
        block_key, tx = idx
        tx_tlb = Transaction().cell_unpack(tx['tx'], True)
        account_address = int(tx_tlb.account_addr, 2)
        account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")

        em, _ = self._get_emulators(block_key)
        # Use global override if present and sync with block state
        state1 = self.global_overrides.get(account_addr) or self.account_states1[block_key][account_addr]
        self.account_states1[block_key][account_addr] = state1

        tmp_out, new_state1, _new_state2, out_msgs = emulate_tx_step(
            self.blocks[block_key], tx, em, None, state1, None, self.loglevel, self.color_schema, True
        )
        out.extend(tmp_out)
        self.account_states1[block_key][account_addr] = new_state1

        # Build node for emulated tx
        mode_info = self._summarize_mode(tmp_out)
        node: Dict[str, Any] = {
            'tx_hash': hex_to_b64(h),
            'in_msg_hash': in_msg_b64,
            'in_msg_body_hash': in_msg_body_b64,
            'opcode': in_opcode,
            'destination': in_destination,
            'bounce': in_bounce,
            'bounced': in_bounced,
            'mode': mode_info.get('mode'),
            **({'diff': mode_info['diff']} if 'diff' in mode_info else {}),
            'children': []
        }

        expected_children_ordered = self.child_map.get(h, [])
        link_map = self.child_link_map.get(h, {})

        emitted_list = out_msgs.get('out_msgs', [])
        i = 0
        for m in emitted_list:
            mh_b64 = hex_to_b64(m['msg_hash'])
            body_b64 = hex_to_b64(m.get('bodyhash')) if m.get('bodyhash') else None
            opcode = m.get('opcode')
            if i < len(expected_children_ordered) and mh_b64 == expected_children_ordered[i]:
                child_hex = link_map.get(mh_b64)
                if child_hex:
                    child_dest = m.get('dest').raw if m.get('dest') is not None else None
                    child_node = self._process_tx(child_hex, out, visited, mh_b64, body_b64, opcode, child_dest, m.get('bounce'), m.get('bounced'))
                    if child_node is not None:
                        node['children'].append(child_node)
                else:
                    expected_meta = (self.message_meta.get(h, {}) or {}).get(mh_b64) or {}
                    dest = expected_meta.get('destination')
                    try:
                        if isinstance(dest, str):
                            dest = Address(dest).raw
                    except Exception:
                        pass
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
                    child_hex = link_map.get(expected_in_b64)
                    expected_dest_raw = None
                    if child_hex:
                        # Fetch expected child's destination from toncenter tx details (in_msg.destination)
                        child_b64 = hex_to_b64(child_hex)
                        child_info = (self.original_tx_details or {}).get(child_b64) or {}
                        in_msg_info = child_info.get('in_msg') or {}
                        expected_dest = in_msg_info.get('destination')
                        try:
                            if isinstance(expected_dest, str):
                                expected_dest_raw = Address(expected_dest).raw
                        except Exception:
                            expected_dest_raw = expected_dest
                    # Compare destinations
                    emitted_dest_raw = m.get('dest').raw if m.get('dest') is not None else None
                    if child_hex and expected_dest_raw is not None and emitted_dest_raw == expected_dest_raw and 'cell' in m:
                        # Emulate the real child tx but override its in_msg with the emitted one
                        child_idx = self.tx_index.get(child_hex)
                        if child_idx is not None:
                            child_block_key, child_tx = child_idx
                            # Prepare child account state
                            child_tlb = Transaction().cell_unpack(child_tx['tx'], True)
                            child_account_int = int(child_tlb.account_addr, 2)
                            child_account_addr = Address(f"{child_block_key[0]}:{hex(child_account_int).upper()[2:].zfill(64)}")
                            em_child, _ = self._get_emulators(child_block_key)
                            child_state = self.global_overrides.get(child_account_addr) or self.account_states1[child_block_key][child_account_addr]
                            self.account_states1[child_block_key][child_account_addr] = child_state
                            # Run override emulation with child's timing
                            tmp_out2, new_state_child, _ns2, child_out_msgs = emulate_tx_step_with_in_msg(
                                self.blocks[child_block_key], child_tx, em_child, child_state, m['cell'], self.loglevel, self.color_schema, True
                            )
                            out.extend(tmp_out2)
                            # Update states and global override
                            self.account_states1[child_block_key][child_account_addr] = new_state_child
                            self.global_overrides[child_account_addr] = new_state_child
                            # Build child node and process its children inline using produced out msgs
                            child_mode = self._summarize_mode(tmp_out2)
                            child_node = {
                                'tx_hash': hex_to_b64(child_hex),
                                'in_msg_hash': mh_b64,
                                'in_msg_body_hash': body_b64,
                                'opcode': opcode,
                                'destination': emitted_dest_raw,
                                'bounce': m.get('bounce'),
                                'bounced': m.get('bounced'),
                                'mode': child_mode.get('mode'),
                                **({'diff': child_mode['diff']} if 'diff' in child_mode else {}),
                                'children': []
                            }
                            # Process child's emitted messages against its expected children with override logic
                            child_nodes = self._process_emitted_children_with_override(
                                child_block_key,
                                child_hex,
                                child_tx,
                                child_out_msgs.get('out_msgs', []),
                                out,
                                visited
                            )
                            child_node['children'].extend(child_nodes)
                            node['children'].append(child_node)
                            # We consumed one expected child position
                            i += 1
                            continue
                # Fallback to previous behavior: treat as extra
                if m is not None and 'cell' in m:
                    child_node = self._emulate_internal_message_recursive(block_key, m, tx['now'], tx['lt'])
                    if child_node is not None:
                        node['children'].append(child_node)
                else:
                    # No cell to emulate; skip adding a node for this extra message.
                    pass

        while i < len(expected_children_ordered):
            missing_b64 = expected_children_ordered[i]
            expected_meta = (self.message_meta.get(h, {}) or {}).get(missing_b64) or {}
            dest3 = expected_meta.get('destination')
            try:
                if isinstance(dest3, str):
                    dest3 = Address(dest3).raw
            except Exception:
                pass
            orig_child_hex = link_map.get(missing_b64)
            orig_child_b64 = hex_to_b64(orig_child_hex) if orig_child_hex else None
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
        missing: List[Dict[str, Any]] = []
        def walk(n):
            if not isinstance(n, dict):
                return
            ih = n.get('in_msg_hash')
            if isinstance(ih, str) and ih not in present:
                # Include the node as-is but ensure schema fields are present
                missing.append({
                    'tx_hash': n.get('tx_hash'),
                    'in_msg_hash': ih,
                    'in_msg_body_hash': n.get('in_msg_body_hash'),
                    'opcode': n.get('opcode'),
                    'destination': n.get('destination'),
                    'bounce': n.get('bounce'),
                    'bounced': n.get('bounced'),
                    'children': []
                })
            for ch in n.get('children', []) or []:
                walk(ch)
        walk(orig)
        return missing

    def run(self, tx_order_hex_upper: List[str]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        order_list = tx_order_hex_upper or self.tx_order_hex_upper
        if not order_list:
            return out
        root_h = order_list[0]
        root_in = self.original_trace_root.get('in_msg_hash') if self.original_trace_root else None
        # Root in_msg body/opcode/destination/bounce flags from original tx details
        root_b64 = hex_to_b64(root_h)
        root_info = (self.original_tx_details or {}).get(root_b64) or {}
        root_in_msg = root_info.get('in_msg') or {}
        root_body = (root_in_msg.get('message_content') or {}).get('hash')
        root_opcode = root_in_msg.get('opcode')
        root_destination = root_in_msg.get('destination')
        root_bounce = root_in_msg.get('bounce')
        root_bounced = root_in_msg.get('bounced')
        try:
            if isinstance(root_destination, str):
                root_destination = Address(root_destination).raw
        except Exception:
            pass
        emu_root = self._process_tx(root_h, out, visited=set(), in_msg_b64=root_in, in_msg_body_b64=root_body, in_opcode=root_opcode, in_destination=root_destination, in_bounce=root_bounce, in_bounced=root_bounced)
        self.emulated_trace_root = emu_root
        # Prepare not_presented list by a second recursive pass
        present_hashes = self._collect_in_hashes(self.emulated_trace_root)
        not_presented = self._collect_not_presented(self.original_trace_root, present_hashes)
        # Record original and emulated trace (no diff) into failed_traces.json
        self.failed_traces.append({
            'type': 'trace_tree_comparison',
            'root_tx': hex_to_b64(root_h),
            'original_trace': self.original_trace_root,
            'emulated_trace': self.emulated_trace_root,
            'not_presented': not_presented,
        })
        return out
