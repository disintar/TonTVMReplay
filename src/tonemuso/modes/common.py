# Shared helpers extracted from main.py to avoid duplication
from collections import OrderedDict, defaultdict
import json
from queue import Empty as QueueEmpty
from typing import Any, Dict, List, Optional, Set, Tuple

from tonpy.blockscanner.blockscanner import *
from tonpy import begin_cell
from tonpy.autogen.block import Transaction
from tonpy.tvm.not_native.emulator_extern import EmulatorExtern
from tonpy import Address
from loguru import logger

from tonemuso.utils import b64_to_hex
from tonemuso.emulation import TxStepEmulator, init_emulators
from tonemuso.trace_models import TxRecord
from tonemuso.trace_runner import TraceOrderedRunner


# No module-level globals; helpers are parameterized by cfg-derived values.


@curry
def process_blocks(data, config_override: dict = None, trace_whitelist: set = None, loglevel: int = 1,
                   color_schema: Optional[Dict[str, Any]] = None, emulator_path: Optional[str] = None,
                   emulator_unchanged_path: Optional[str] = None, txs_whitelist: Optional[Set[str]] = None):
    out = []
    block, initial_account_state, txs = data

    # Base/working configs
    base_config: VmDict = VmDict(32, False, block['key_block']['config'])
    config: VmDict = VmDict(32, False, block['key_block']['config'])
    if config_override is not None:
        for param in config_override:
            config.set(int(param), begin_cell().store_ref(Cell(config_override[param])).end_cell().begin_parse())

    # Emulators
    em = EmulatorExtern(emulator_path, config)
    em.set_rand_seed(block['rand_seed'])
    prev_block_data = [list(reversed(block['prev_block_data'][1])), block['prev_block_data'][2],
                       list(reversed(block['prev_block_data'][0]))]
    em.set_prev_blocks_info(prev_block_data)
    em.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    em2 = EmulatorExtern(emulator_unchanged_path, base_config)
    em2.set_rand_seed(block['rand_seed'])
    em2.set_prev_blocks_info(prev_block_data)
    em2.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    # Filtering
    effective_filter = trace_whitelist or txs_whitelist
    if effective_filter is not None:
        process_this_chunk = any(tx['tx'].get_hash() in effective_filter for tx in txs)
        if not process_this_chunk:
            return []

    # Iterate
    account_state_em1 = initial_account_state
    account_state_em2 = initial_account_state
    step = TxStepEmulator(block=block, loglevel=loglevel, color_schema=color_schema, em=em,
                          account_state_em1=account_state_em1, em2=em2, account_state_em2=account_state_em2)
    for tx in txs:
        try:
            if txs_whitelist is not None and tx['tx'].get_hash() not in txs_whitelist:
                _out, account_state_em1, _ns2, _om = step.emulate(tx, extract_out_msgs=False)
                account_state_em2 = _ns2 if _ns2 is not None else account_state_em2
                continue
            tmp_out, account_state_em1, _ns2, _om = step.emulate(tx, extract_out_msgs=False)
            account_state_em2 = _ns2 if _ns2 is not None else account_state_em2
            out.extend(tmp_out)
        except Exception as e:
            logger.error(f"EMULATOR ERROR: Got {e} while emulating!")
            raise e
    return out


@curry
def collect_raw(data, trace_tx_hashes_hex: Set[str], config_override: dict = None, loglevel: int = 1, emulator_path: Optional[str] = None,
                emulator_unchanged_path: Optional[str] = None):
    block, initial_account_state, txs = data

    # Build TxRecord objects and, if a non-empty trace set is provided, attach non-trace preceding txs
    tx_objs: List[TxRecord] = []
    buffer: List[TxRecord] = []  # holds non-trace txs until the next in-trace tx
    use_buffer = bool(trace_tx_hashes_hex) and len(trace_tx_hashes_hex) > 0
    for t in txs:
        if isinstance(t, dict):
            rec = TxRecord(tx=t['tx'], lt=t['lt'], now=t['now'], is_tock=t['is_tock'])
        else:
            rec = t
        if use_buffer:
            if rec.tx.get_hash().upper() in trace_tx_hashes_hex:
                rec.before_txs = list(buffer)
                buffer.clear()
            else:
                buffer.append(rec)
        tx_objs.append(rec)

    em, em2 = init_emulators(block, config_override, emulator_path=emulator_path,
                             emulator_unchanged_path=emulator_unchanged_path)

    account_state_em1 = initial_account_state
    account_state_em2 = initial_account_state

    step = TxStepEmulator(block=block, loglevel=loglevel, color_schema=None, em=em, account_state_em1=account_state_em1,
                          em2=em2, account_state_em2=account_state_em2)
    for tx in tx_objs:
        before_state_em2 = account_state_em2
        _out, account_state_em1, _ns2, _ = step.emulate(tx, extract_out_msgs=False)
        account_state_em2 = _ns2 if _ns2 is not None else account_state_em2
        tx.before_state_em2 = before_state_em2
        tx.after_state_em2 = account_state_em2
        tx.unchanged_emulator_tx_hash = em2.transaction.get_hash() if em2.transaction is not None else None

    return [(block, initial_account_state, tx_objs)]


def process_result(outq, loglevel: int = 1):
    total_txs = []
    while True:
        try:
            total_txs.append(outq.get_nowait())
        except QueueEmpty:
            break

    tmp_s = 0
    tmp_w = 0
    tmp_u = []
    if len(total_txs) > 0:
        for chunk in total_txs:
            for i in chunk:
                if i['mode'] == 'success':
                    tmp_s += 1
                elif i['mode'] == 'warning':
                    tmp_w += 1
                else:
                    tmp_u.append(i)

    if loglevel > 1 and (tmp_s or tmp_w or tmp_u):
        logger.warning(f"Emulator status: {tmp_s} success, {tmp_w} warnings, {len(tmp_u)} errors")

    return tmp_s, tmp_u, tmp_w


# Worker helpers for multi-trace mode
_W_PREINDEX = None
_W_LCPARAMS = None
_W_LOGLEVEL = None
_W_COLOR_SCHEMA = None
_W_C7_ENV = None
_W_EMULATOR_PATH = None
_W_EMULATOR_UNCHANGED_PATH = None


def worker_init(preindexed, lcparams, loglevel, color_schema, c7_env, emulator_path, emulator_unchanged_path):
    global _W_PREINDEX, _W_LCPARAMS, _W_LOGLEVEL, _W_COLOR_SCHEMA, _W_C7_ENV, _W_EMULATOR_PATH, _W_EMULATOR_UNCHANGED_PATH
    _W_PREINDEX = preindexed
    _W_LCPARAMS = lcparams
    _W_LOGLEVEL = loglevel
    _W_COLOR_SCHEMA = color_schema
    _W_C7_ENV = c7_env
    _W_EMULATOR_PATH = emulator_path
    _W_EMULATOR_UNCHANGED_PATH = emulator_unchanged_path


def process_one_trace_worker(args):
    try:
        tidx, t = args
        config_override = json.loads(_W_C7_ENV) if _W_C7_ENV else None
        # t must be a TonTrace instance
        tx_order = t.transactions_order_b64
        tx_order_list = [b64_to_hex(h).upper() for h in (tx_order or [])]
        runner_local = TraceOrderedRunner(
            raw_chunks=None,
            config_override=config_override,
            loglevel=_W_LOGLEVEL,
            color_schema=_W_COLOR_SCHEMA,
            tx_order_hex_upper=tx_order_list or [],
            toncenter_trace=t,
            lcparams=_W_LCPARAMS,
            preindexed=_W_PREINDEX,
            emulator_path=_W_EMULATOR_PATH,
            emulator_unchanged_path=_W_EMULATOR_UNCHANGED_PATH
        )
        runner_local.run(tx_order_list or [])
        return runner_local.failed_traces or []
    except Exception as e:
        try:
            logger.error(f"Error processing trace #{args[0]} in worker: {e}")
        except Exception:
            pass
        return []


def build_preindex(raw_chunks):
    blocks = {}
    tx_index = {}
    before_states = {}
    default_initial_state = defaultdict(OrderedDict)
    for block, initial_account_state, txs in raw_chunks:
        blk = block['block_id']
        block_key = (blk.id.workchain, blk.id.shard, blk.id.seqno, blk.root_hash)
        blocks[block_key] = block
        for tx in txs:
            try:
                txh = tx.tx.get_hash().upper()
                tx_index[txh] = (block_key, tx)
                tx_tlb = Transaction().cell_unpack(tx.tx, True)
                account_address = int(tx_tlb.account_addr, 2)
                account_addr = Address(f"{block_key[0]}:{hex(account_address).upper()[2:].zfill(64)}")
                if account_addr not in default_initial_state[block_key]:
                    default_initial_state[block_key][account_addr] = initial_account_state
                before_states[txh] = tx.before_state_em2
            except Exception:
                pass
    return {
        'blocks': blocks,
        'tx_index': tx_index,
        'before_states': before_states,
        'default_initial_state': default_initial_state,
    }
