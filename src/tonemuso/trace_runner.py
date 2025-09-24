# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import defaultdict, OrderedDict

from tonpy import Cell, LiteClient, begin_cell, BlockId, Address
from tonpy.tvm.not_native.emulator_extern import EmulatorExtern
from tonpy.autogen.block import Transaction, Block, BlockInfo
from loguru import logger
from tqdm import tqdm

from tonemuso.emulation import init_emulators, TxStepEmulator
from tonemuso.emitted_processor import EmittedMessageProcessor, EmittedChildCandidate
from tonemuso.utils import b64_to_hex, hex_to_b64
from tonemuso.trace_models import MessageMeta, EmuMessage, EmulatedNode, TxRecord
from tonemuso.diff import get_shard_account_diff, make_json_dumpable

from tonemuso.emulation import extract_message_info
from tonemuso.utils import count_modes_tree
from tonemuso.toncenter_models import *

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
                 lcparams: Optional[Dict[str, Any]] = None,
                 preindexed: Optional[Dict[str, Any]] = None,
                 emulator_path: Optional[str] = None,
                 emulator_unchanged_path: Optional[str] = None,
                 toncenter_trace: "TonTrace" = None,
                 use_boc_for_diff: bool = False):
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
        self.emulator_path = emulator_path
        self.emulator_unchanged_path = emulator_unchanged_path

        # Diff behavior flag for transaction comparison
        self.use_boc_for_diff: bool = bool(use_boc_for_diff)

        # Results and global state overrides
        self.failed_traces: List[Dict[str, Any]] = []
        self.global_overrides: Dict[Address, Cell] = OrderedDict()
        # Track which original expected children were actually used for comparison
        # Each entry is a tuple (parent_tx_hash_hex, expected_in_msg_b64)
        self.used_original_pairs: Set[Tuple[TxHashHex, MsgHashB64]] = set()
        # Emulation order counter for nodes we actually emulate
        self.emulation_counter: int = 0

        # Store Toncenter typed trace for lookups (no legacy dicts)
        if toncenter_trace is None:
            raise ValueError("toncenter_trace must be provided")
        self.toncenter_trace: TonTrace = toncenter_trace
        tx_map_dict: Optional[Dict[str, Any]] = None
        if self.toncenter_trace.trace is not None:
            tx_map_dict = self.toncenter_trace.trace.node_to_dict()

        self._build_message_meta()

        # Child maps built from toncenter trace map (will be sorted by in_msg.created_lt)
        self.child_map: Dict[TxHashHex, List[MsgHashB64]] = OrderedDict()
        self.child_link_map: Dict[TxHashHex, Dict[MsgHashB64, TxHashHex]] = OrderedDict()
        self._build_child_maps(tx_map_dict)

        # Keep original trace root for reporting (transformed to required TX schema)
        self.original_trace_root: Optional[Dict[str, Any]] = None
        self.emulated_trace_root: Optional[Dict[str, Any]] = None
        if getattr(toncenter_trace, 'trace', None) is not None:
            self.original_trace_root = toncenter_trace.to_tx_schema_root()

        # Build raw indices for quick access (from preindexed or raw chunks)
        if preindexed is not None:
            try:
                # Copy indices into per-runner containers (avoid sharing mutable maps across traces)
                # Rebuild tx_index with per-runner TxRecord clones to avoid accidental mutation sharing
                self.tx_index = OrderedDict()
                for h, pair in (preindexed.get('tx_index') or {}).items():
                    bk, tx = pair
                    # Clone TxRecord shallowly (do not carry before_txs buffer across runs)
                    tx_cloned = TxRecord(tx=tx.tx, lt=tx.lt, now=tx.now, is_tock=tx.is_tock)
                    tx_cloned.before_state_em2 = tx.before_state_em2
                    tx_cloned.after_state_em2 = tx.after_state_em2
                    tx_cloned.unchanged_emulator_tx_hash = tx.unchanged_emulator_tx_hash
                    self.tx_index[h] = (bk, tx_cloned)
                self.blocks = OrderedDict(preindexed.get('blocks') or {})
                # Do not pass emulators
                self.account_states1 = defaultdict(OrderedDict)
                # Default initial state per block per account (for lazy seeding)
                # Build a fresh defaultdict with shallow-copied OrderedDict values to avoid cross-trace mutations
                self.default_initial_state = defaultdict(OrderedDict)
                for bk, addr_map in (preindexed.get('default_initial_state') or {}).items():
                    # Shallow copy mapping; Cells are treated as values and not mutated by emulators
                    self.default_initial_state[bk] = OrderedDict(addr_map)
                # BEFORE states (per-tx) – copy keys to avoid accidental mutation sharing
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
    def _build_message_meta(self) -> None:
        # Build mapping: parent transaction_hash (hex) -> { in_msg_b64 -> MessageMeta }
        self.message_meta: Dict[TxHashHex, Dict[MsgHashB64, MessageMeta]] = OrderedDict()
        for b64_tx, txd in self.toncenter_trace.transactions.items():
            parent_transaction_hash = b64_to_hex(b64_tx)
            out_list = txd.out_msgs
            if not out_list:
                continue
            meta_map: Dict[MsgHashB64, MessageMeta] = OrderedDict()
            for m in out_list:
                in_b64 = m.hash_b64  # outgoing message hash in base64
                if not isinstance(in_b64, str):
                    continue
                meta_map[in_b64] = MessageMeta(
                    opcode=m.opcode,
                    destination_raw=m.destination_raw,
                    body_hash_b64=m.body_hash,
                    bounce=m.bounce,
                    bounced=m.bounced
                )

            if meta_map:
                self.message_meta[parent_transaction_hash] = meta_map

    def _build_child_maps(self, toncenter_tx_map: Optional[Dict[str, Any]]) -> None:
        if not toncenter_tx_map:
            return

        # If we were given a single trace tree node (has tx_hash and children), walk it recursively
        def walk(node: Dict[str, Any]):
            parent_transaction_hash = b64_to_hex(node.get('tx_hash')) if node.get('tx_hash') else None
            children = node.get('children') or []

            # Sort children by their in_msg.created_lt from typed Toncenter models to ensure deterministic order
            def child_created_lt(ch_node: Dict[str, Any]) -> int:
                tx_b64 = ch_node.get('tx_hash')
                if isinstance(tx_b64, str):
                    txd = self.toncenter_trace.transactions.get(tx_b64)
                    if txd and txd.in_msg and txd.in_msg.created_lt is not None:
                        return int(txd.in_msg.created_lt)
                return 0

            children_sorted = sorted([c for c in children if isinstance(c, dict)], key=child_created_lt)

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

    def _index_raw_chunks(self, raw_chunks: List[Tuple[Dict[str, Any], Cell, List[Dict[str, Any]]]]) -> None:
        for block, initial_account_state, txs in raw_chunks:
            blk = block['block_id']
            block_key: BlockKey = (blk.id.workchain, blk.id.shard, blk.id.seqno, blk.root_hash)
            self.blocks[block_key] = block
            # Record default initial state per account in this block (for lazy seeding)
            # Note: raw_chunks provide initial_account_state corresponding to the account of txs in this chunk.
            # We will assign it per-account below when we parse txs.
            for tx in txs:
                tx_tlb = Transaction().cell_unpack(tx.tx, True)
                account_address = int(tx_tlb.account_addr, 2)
                account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")
                # Seed default account state maps
                if account_addr not in self.default_initial_state[block_key]:
                    self.default_initial_state[block_key][account_addr] = initial_account_state
                if account_addr not in self.account_states1[block_key]:
                    self.account_states1[block_key][account_addr] = initial_account_state
                transaction_hash = tx.tx.get_hash().upper()
                self.tx_index[transaction_hash] = (block_key, tx)
                # Capture BEFORE state provided by collect_raw (mandatory from em2)
                self.before_states[transaction_hash] = tx.before_state_em2

    def _init_liteclient(self, lcparams: Optional[Dict[str, Any]]) -> None:
        p = dict(lcparams)
        p['logprefix'] = 'tracerunner'
        self.lc = LiteClient(**p)

    # ---------- Emulator / state helpers ----------
    def _next_emulation_order(self) -> int:
        self.emulation_counter += 1
        return self.emulation_counter

    def _assign_depths(self, node: Dict[str, Any], depth: int) -> None:
        assert isinstance(node, dict)
        node['depth'] = int(depth)
        for ch in (node.get('children') or []):
            self._assign_depths(ch, depth + 1)

    def _get_emulators(self, block_key: BlockKey) -> Tuple[EmulatorExtern, EmulatorExtern]:
        if block_key not in self.block_emulators:
            em, em2 = init_emulators(self.blocks[block_key], self.config_override, emulator_path=self.emulator_path,
                                     emulator_unchanged_path=self.emulator_unchanged_path)
            self.block_emulators[block_key] = (em, em2)
        return self.block_emulators[block_key]

    def _fetch_state_for_account(self, block_key: BlockKey, account_addr: Address) -> Optional[Cell]:
        if self.lc is None:
            return None
        block = self.blocks[block_key]

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

    def _ensure_account_state(self, block_key: BlockKey, account_addr: Address) -> None:
        if account_addr in self.account_states1[block_key]:
            return
        st = self._fetch_state_for_account(block_key, account_addr)
        if st is not None:
            self.account_states1[block_key][account_addr] = st
        else:
            logger.warning(f"No state available for account {str(account_addr)} in block {block_key}")

    def _process_emitted_children_with_override(self,
                                                parent_block_key: BlockKey,
                                                parent_transaction_hash: TxHashHex,
                                                parent_tx: "TxRecord",
                                                emitted_list: List[EmittedChildCandidate],
                                                out: List[Dict[str, Any]],
                                                visited: Set[TxHashHex]) -> Tuple[List[Dict[str, Any]], List[
        Tuple[BlockKey, TxHashHex, "TxRecord", List[EmittedChildCandidate], Dict[str, Any]]]]:
        processor = EmittedMessageProcessor(self)
        return processor.process_emitted_children_with_override(parent_block_key, parent_transaction_hash, parent_tx,
                                                                emitted_list, out, visited)

    # ---------- Traversal ----------
    def _process_tx(self, transaction_hash: TxHashHex,
                    out: List[Dict[str, Any]],
                    visited: Set[TxHashHex],
                    in_msg_ref: TonMessageRef) -> Optional[Dict[str, Any]]:
        if transaction_hash in visited:
            return None
        visited.add(transaction_hash)
        idx = self.tx_index.get(transaction_hash)
        if idx is None:
            logger.warning(f"Transaction from order not found in collected data: {transaction_hash}")
            return None
        block_key, tx = idx
        tx_tlb = Transaction().cell_unpack(tx.tx, True)
        account_address = int(tx_tlb.account_addr, 2)
        account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")

        em, em2 = self._get_emulators(block_key)
        # Determine BEFORE state and record its source for diagnostics in failed_traces.json
        state1 = None
        state1_source = None
        if transaction_hash in self.before_states:
            state1 = self.before_states[transaction_hash]
            state1_source = 'before_states'
        elif account_addr in self.account_states1[block_key]:
            state1 = self.account_states1[block_key][account_addr]
            state1_source = 'account_states1'
        else:
            dflt = (self.default_initial_state.get(block_key, {}) or {}).get(account_addr)
            if dflt is not None:
                state1 = dflt
                state1_source = 'default_initial_state'
            else:
                state1 = self._fetch_state_for_account(block_key, account_addr)
                state1_source = 'fetched_liteclient' if state1 is not None else 'none'
        # Sync the stored account state to the chosen BEFORE state
        self.account_states1[block_key][account_addr] = state1

        # Emulate root tx via unified stepper
        step = TxStepEmulator(block=self.blocks[block_key], loglevel=self.loglevel, color_schema=self.color_schema,
                              em=em, account_state_em1=state1, em2=em2, account_state_em2=state1,
                              use_boc_for_diff=self.use_boc_for_diff)
        tmp_out, new_state1, _new_state2, out_msgs = step.emulate(
            tx,
            extract_out_msgs=True
        )

        out.extend(tmp_out)
        self.account_states1[block_key][account_addr] = new_state1

        # Build node for emulated tx using unified EmulatedNode; derive all in_msg fields internally
        node_obj = EmulatedNode(
            tx_hash_hex=transaction_hash,
            children=[],
            tmp_out=tmp_out,
            in_msg=in_msg_ref
        )
        node = node_obj.to_dict()
        # Annotate diagnostics: where BEFORE state was taken from and which account it was

        node['account_state_source'] = state1_source
        node['account_address'] = str(account_addr)
        # Assign emulation order for the root node
        node['emulation_order'] = self._next_emulation_order()
        # Depth for root
        self._assign_depths(node, 0)

        # Delegate processing of emitted children to the unified helper to avoid duplication
        emitted_list_raw = out_msgs.get('out_msgs', [])
        processor = EmittedMessageProcessor(self)
        emitted_candidates = processor.classify_emitted(transaction_hash, emitted_list_raw)
        # DFS over emitted children by layers
        # Worklist items: (parent_block_key, parent_tx_hash, parent_tx, candidates, parent_node_dict)
        worklist: List[Tuple[BlockKey, TxHashHex, TxRecord, List[EmittedChildCandidate], Dict[str, Any]]] = [
            (block_key, transaction_hash, tx, emitted_candidates, node)
        ]
        while worklist:
            # Process all current items and collect next-layer contexts
            next_contexts_all: List[
                Tuple[BlockKey, TxHashHex, TxRecord, List[EmittedChildCandidate], Dict[str, Any]]] = []
            for pbk, ptxh, ptx, cands, pnode in worklist:
                child_nodes, next_contexts = self._process_emitted_children_with_override(
                    pbk, ptxh, ptx, cands, out, visited
                )
                # Assign depths to new child nodes (and their subtrees if any)
                parent_depth = int(pnode.get('depth') or 0)
                for ch in child_nodes:
                    self._assign_depths(ch, parent_depth + 1)
                # Append built child nodes to this parent node
                pnode['children'].extend(child_nodes)
                # Accumulate next contexts (each carries its own gc_node)
                next_contexts_all.extend(next_contexts)

            # # Sort next layer contexts by their candidates' original_order_idx (min across list)
            # def ctx_key(ctx_item):
            #     _, _txh, _tx, _cands, _pnode = ctx_item
            #     INF = 10 ** 18
            #     if not _cands:
            #         return INF
            #
            #     return min(int(c.original_order_idx) if c.original_order_idx is not None else INF for c in _cands)
            #
            # next_contexts_all.sort(key=ctx_key)
            worklist = next_contexts_all
        return node

    def _collect_not_presented(self, orig: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Determine original transactions that were not used for comparison.
        We mark a child as 'used' if, during comparison, we actually processed its real transaction
        (exact in_msg match or position+destination override). Anything else is considered not presented.
        """
        missing: List[Dict[str, Any]] = []

        def walk(node: Optional[Dict[str, Any]]):
            if not isinstance(node, dict):
                return
            parent_b64 = node.get('tx_hash')
            parent_hex = None
            if isinstance(parent_b64, str):
                parent_hex = b64_to_hex(parent_b64)
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
        # Reset per-run caches to ensure full isolation between traces when a runner is reused
        self.used_original_pairs = set()
        self.global_overrides = OrderedDict()
        self.emulation_counter = 0
        self.block_emulators = OrderedDict()
        self.account_states1 = defaultdict(OrderedDict)
        self.failed_traces = []
        self.emulated_trace_root = None

        out: List[Dict[str, Any]] = []
        order_list = tx_order_hex_upper or self.tx_order_hex_upper
        if not order_list:
            return out
        root_transaction_hash = order_list[0]
        # Fetch root tx details and pass typed in_msg directly
        root_b64 = hex_to_b64(root_transaction_hash)
        txd_root = self.toncenter_trace.transactions.get(root_b64)
        emu_root = self._process_tx(
            root_transaction_hash,
            out,
            visited=set(),
            in_msg_ref=txd_root.in_msg)

        self.emulated_trace_root = emu_root
        # Prepare not_presented list by a second recursive pass
        not_presented = self._collect_not_presented(self.original_trace_root)

        # Record original and emulated trace (no diff) into failed_traces.json
        # Compute final status breakdown using shared utils
        counts = count_modes_tree(self.emulated_trace_root or {})
        # missed also includes original children not presented at all
        missed_total = counts.get('missed', 0) + len(not_presented)

        # Helper: normalize opcode to int if possible
        def _norm_op(v) -> Optional[int]:
            if v is None:
                return None
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                s = v.strip().lower()
                if s.startswith('0x'):
                    return int(s, 16)
                return int(s)

            return None

        # Check if extra new tx is allowed by color schema (diff_colored.json)
        # Return number of allowed "new_transaction" nodes (0 if not allowed)
        def _allowed_extra_new_count(root_node) -> int:
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
            new_roots = [i for i in new_indices if
                         parent_of.get(i) is None or flat[parent_of[i]].get('mode') != 'new_transaction']
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

        allowed_extra_applied = False
        allowed_extra_new = 0
        if self.emulated_trace_root is not None:
            allowed_extra_new = _allowed_extra_new_count(self.emulated_trace_root)
            allowed_extra_applied = allowed_extra_new > 0

        # Detect any emulation_failed nodes in the emulated tree
        def _has_emulation_failed(n) -> bool:
            if not isinstance(n, dict):
                return False
            if n.get('mode') == 'emulation_failed':
                return True
            for ch in (n.get('children') or []):
                if _has_emulation_failed(ch):
                    return True
            return False

        has_emulation_failed = _has_emulation_failed(self.emulated_trace_root or {})

        final_success = (
                (self.emulated_trace_root is not None)
                and counts.get('warnings', 0) == 0
                and counts.get('unsuccess', 0) == 0
                and (counts.get('new', 0) - allowed_extra_new) == 0
                and missed_total == 0
                and not has_emulation_failed
        )

        # Decide final status: success, warning (only warnings), or error (unsuccess/new/missed or emulation_failed present)
        if final_success:
            final_status = 'success'
        else:
            only_warnings = (
                    counts.get('unsuccess', 0) == 0 and
                    (counts.get('new', 0) - allowed_extra_new) == 0 and
                    missed_total == 0 and
                    counts.get('warnings', 0) > 0 and
                    not has_emulation_failed
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
