# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Optional

from tonpy.blockscanner.blockscanner import *
from tonpy import begin_cell
from collections import Counter
import json
import os
import argparse
import base64
import requests
from multiprocessing import get_context
from tqdm import tqdm
from tonemuso.diff import get_diff, get_colored_diff, make_json_dumpable, get_shard_account_diff
from queue import Empty as QueueEmpty
from tonemuso.trace_runner import TraceOrderedRunner
from tonemuso.utils import b64_to_hex, hex_to_b64
from tonpy.autogen.block import Block, BlockInfo

LOGLEVEL = int(os.getenv("EMUSO_LOGLEVEL", 1))
COLOR_SCHEMA = str(os.getenv("COLOR_SCHEMA_PATH", ''))

if len(COLOR_SCHEMA):
    with open(COLOR_SCHEMA, "r") as f:
        COLOR_SCHEMA = json.load(f)
else:
    COLOR_SCHEMA = None

TXS_TO_PROCESS = os.getenv("TXS_TO_PROCESS_PATH", None)
TXS_WHITELIST = None
if TXS_TO_PROCESS is not None:
    with open(TXS_TO_PROCESS, 'r') as f:
        TXS_TO_PROCESS = json.load(f)['transactions']

    TXS_WHITELIST = set()
    for tx in TXS_TO_PROCESS:
        TXS_WHITELIST.add(tx['hash'])


def collect_raw(data):
    # Emulate transactions to attach BEFORE state for emulator1 and AFTER state for emulator2 per tx (trace mode)
    block, initial_account_state, txs = data

    # Initialize emulators similar to process_blocks, respecting optional C7_REWRITE
    try:
        from tonemuso.emulation import init_emulators, emulate_tx_step
        cfg_env = os.getenv("C7_REWRITE", None)
        cfg_override = json.loads(cfg_env) if cfg_env else None
        em, em2 = init_emulators(block, cfg_override)
    except Exception as e:
        logger.error(f"Failed to init emulators in collect_raw: {e}")
        # Fallback: return raw data unchanged
        return [data]

    account_state_em1 = initial_account_state
    account_state_em2 = initial_account_state

    for tx in txs:
        try:
            # BEFORE state for primary emulator
            before_state_em1 = account_state_em1
            # Emulate one step to advance both emulators
            _out, account_state_em1, account_state_em2 = emulate_tx_step(
                block,
                tx,
                em,
                em2,
                account_state_em1,
                account_state_em2,
                LOGLEVEL,
                COLOR_SCHEMA
            )
            # Attach BEFORE/AFTER states and unchanged emulator tx hash (if available)
            tx['before_state_em1'] = before_state_em1
            tx['after_state_em2'] = account_state_em2
            try:
                tx['unchanged_emulator_tx_hash'] = em2.transaction.get_hash() if (
                        em2 is not None and em2.transaction is not None) else None
            except Exception:
                tx['unchanged_emulator_tx_hash'] = None
        except Exception as e:
            logger.error(f"EMULATOR ERROR in collect_raw: {e}")
            # Attach best-known states and continue
            tx['before_state_em1'] = account_state_em1
            tx['after_state_em2'] = account_state_em2
            continue

    # Return as a list so BlockScanner puts it into out_queue
    return [(block, initial_account_state, txs)]


@curry
def process_blocks(data, config_override: dict = None, trace_whitelist: set = None):
    global TXS_WHITELIST

    if isinstance(TXS_WHITELIST, set) and trace_whitelist is not None:
        raise ValueError("TXS_WHITELIST and txs_whitelist are mutually exclusive")

    out = []
    block, initial_account_state, txs = data
    if LOGLEVEL > 3:
        logger.debug(f"Start process block TXs: {len(txs)}")

    # Base config from block key_block (no overrides)
    base_config: VmDict = VmDict(32, False, block['key_block']['config'])

    # Working config (may be overridden via C7_REWRITE)
    config: VmDict = VmDict(32, False, block['key_block']['config'])
    if config_override is not None:
        for param in config_override:
            config.set(int(param), begin_cell().store_ref(Cell(config_override[param])).end_cell().begin_parse())

    if LOGLEVEL > 3:
        logger.debug(f"Init emulator(s)")

    # Primary emulator (possibly with C7 rewrite)
    em = EmulatorExtern(os.getenv("EMULATOR_PATH"), config)
    em.set_rand_seed(block['rand_seed'])

    prev_block_data = [list(reversed(block['prev_block_data'][1])),  # prev 16
                       block['prev_block_data'][2],  # key block
                       list(reversed(block['prev_block_data'][0]))]  # prev 16 by 100

    em.set_prev_blocks_info(prev_block_data)
    em.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    # Secondary emulator (unchanged), optional
    em2 = None
    unchanged_path = os.getenv("EMULATOR_UNCHANGED_PATH", "")
    if unchanged_path:
        em2 = EmulatorExtern(unchanged_path, base_config)
        em2.set_rand_seed(block['rand_seed'])
        em2.set_prev_blocks_info(prev_block_data)
        em2.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    if LOGLEVEL > 3:
        logger.debug(f"Emulator init success")

    if TXS_WHITELIST is not None or trace_whitelist is not None:
        _filter = trace_whitelist or TXS_WHITELIST
        process_this_chunk = False

        for tx in txs:
            if tx['tx'].get_hash() in _filter:
                process_this_chunk = True
                break

        if not process_this_chunk:
            return []

    if LOGLEVEL > 4:
        txs = tqdm(txs, desc="Emulate accounts")

    # Maintain separated account states for both emulators
    account_state_em1 = initial_account_state
    account_state_em2 = initial_account_state

    from tonemuso.emulation import emulate_tx_step

    for tx in txs:
        try:
            # If TX whitelist provided skip checks but advance both emulator states
            if TXS_WHITELIST is not None and tx['tx'].get_hash() not in TXS_WHITELIST:
                tmp_out, account_state_em1, account_state_em2 = emulate_tx_step(block, tx, em, em2,
                                                                                account_state_em1,
                                                                                account_state_em2,
                                                                                LOGLEVEL, COLOR_SCHEMA)
                if LOGLEVEL > 4:
                    logger.debug(f"Skip checks, not in whitelist")
                # Do not append results when skipping checks
                continue

            tmp_out, account_state_em1, account_state_em2 = emulate_tx_step(block, tx, em, em2,
                                                                            account_state_em1, account_state_em2,
                                                                            LOGLEVEL, COLOR_SCHEMA)
            out.extend(tmp_out)
        except Exception as e:
            logger.error(f"EMULATOR ERROR: Got {e} while emulating!")
            raise e
    return out


def process_result(outq):
    total_txs = []
    # Drain queue without relying on .empty(), which is unreliable for multiprocessing Queues
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

        if LOGLEVEL > 1:
            logger.warning(f"Emulator status: {tmp_s} success, {tmp_w} warnings, {len(tmp_u)} errors")

        if len(tmp_u) > 0 and LOGLEVEL > 1:
            cnt = Counter()
            for i in tmp_u:
                cnt[i['address']] += 1

            logger.error(f"Unique addreses errors: {len(cnt)}, most common: ")
            logger.error(cnt.most_common(5))

    return tmp_s, tmp_u, tmp_w


def _process_one_trace_worker(args):
    """
    Multiprocessing worker to emulate a single trace with TraceOrderedRunner.
    args: (tidx, trace_obj, raw_chunks_all, lcparams, loglevel, color_schema, c7_env_str)
    Returns: list of failed_traces entries (may be empty).
    """
    try:
        tidx, t, raw_chunks_all, lcparams, loglevel, color_schema, c7_env = args
        config_override = json.loads(c7_env) if c7_env else None
        tx_order = t.get("transactions_order", [])
        tx_order_list = [b64_to_hex(h).upper() for h in tx_order]
        runner_local = TraceOrderedRunner(
            raw_chunks=raw_chunks_all,
            config_override=config_override,
            loglevel=loglevel,
            color_schema=color_schema,
            tx_order_hex_upper=tx_order_list or [],
            toncenter_tx_map=t.get("trace"),
            toncenter_tx_details=t.get("transactions", {}),
            lcparams=lcparams
        )
        runner_local.run(tx_order_list or [])
        return runner_local.failed_traces or []
    except Exception as e:
        try:
            logger.error(f"Error processing trace #{args[0]} in worker: {e}")
        except Exception:
            pass
        return []


def main():
    # Use environment variables for configuration (no CLI flags for tx/msg hash / toncenter)
    toncenter_tx_hash = os.getenv("TONCENTER_TX_HASH", None)
    toncenter_msg_hash = os.getenv("TONCENTER_MSG_HASH", None)
    toncenter_api_key = os.getenv("TONCENTER_API_KEY", None)
    toncenter_api = os.getenv("TONCENTER_API", "https://toncenter.com/api/v3")

    server = {
        "ip": int(os.getenv("LITESERVER_SERVER")),
        "port": int(os.getenv("LITESERVER_PORT")),
        "id": {
            "@type": "pub.ed25519",
            "key": os.getenv("LITESERVER_PUBKEY")
        }
    }

    lcparams = {
        'mode': 'roundrobin',
        'my_rr_servers': [server],
        'timeout': float(os.getenv('LITESERVER_TIMEOUT', 5)),
        'num_try': 3000,
        'threads': 1,
        'loglevel': max(LOGLEVEL - 3, 0)
    }

    lc = LiteClient(**lcparams)
    latest_seqno = lc.get_masterchain_info_ext().last.id.seqno
    to_seqno = int(os.getenv("TO_SEQNO", latest_seqno))
    from_seqno = os.getenv("FROM_SEQNO", None)

    if from_seqno is None:
        from_seqno = to_seqno - int(os.getenv("TO_EMULATE_MC_BLOCKS", 10))
    else:
        from_seqno = int(from_seqno)

    # Multi-trace by masters: if TONCENTER_TRACES_BY_MASTERS is set, fetch and run traces in the LT range
    if os.getenv("TONCENTER_TRACES_BY_MASTERS"):
        try:
            # Resolve masterchain blocks to get start_lt and end_lt
            # Masterchain shard is 0x8000000000000000 and workchain is -1
            start_blk_id_ext = lc.lookup_block(BlockId(-1, 0x8000000000000000, int(from_seqno))).blk_id
            end_blk_id_ext = lc.lookup_block(BlockId(-1, 0x8000000000000000, int(to_seqno))).blk_id

            start_blk_cell = lc.get_block(start_blk_id_ext)
            end_blk_cell = lc.get_block(end_blk_id_ext)

            start_blk = Block().cell_unpack(start_blk_cell)
            end_blk = Block().cell_unpack(end_blk_cell)

            start_info = BlockInfo().cell_unpack(start_blk.info, True)
            end_info = BlockInfo().cell_unpack(end_blk.info, True)

            start_lt = int(start_info.start_lt)
            end_lt = int(end_info.end_lt)
        except Exception as e:
            logger.error(f"Failed to compute start/end lt via liteserver for seqno range {from_seqno}-{to_seqno}: {e}")
            return

        toncenter_api_key = os.getenv("TONCENTER_API_KEY", None)
        toncenter_api = os.getenv("TONCENTER_API", "https://toncenter.com/api/v3")
        headers = None
        if toncenter_api_key:
            headers = {
                'X-API-Key': toncenter_api_key,
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
        url = f"{toncenter_api.rstrip('/')}/traces"

        all_traces = []
        offset = 0
        limit = 1000
        downloaded = 0
        pages = 0
        pbar = tqdm(total=None, unit="tr", desc="Downloading traces", disable=False)
        while True:
            params = {
                'start_lt': start_lt,
                'end_lt': end_lt,
                'include_actions': 'false',
                'limit': limit,
                'offset': offset,
                'sort': 'desc'
            }
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Failed to query toncenter traces page offset={offset}: {e}")
                break

            page_traces = []
            if isinstance(data, dict):
                page_traces = data.get('traces', []) or []
            if not page_traces:
                if LOGLEVEL > 1:
                    logger.info(f"No more traces. Pages fetched: {pages}, total downloaded: {downloaded}")
                break
            all_traces.extend(page_traces)
            downloaded += len(page_traces)
            pages += 1
            if LOGLEVEL > 1:
                logger.info(
                    f"Fetched page {pages} (offset={offset}) with {len(page_traces)} traces; total so far: {downloaded}")
            pbar.update(len(page_traces))
            offset += limit
        try:
            pbar.close()
        except Exception:
            pass

        if not all_traces:
            logger.error("No traces returned from toncenter for given LT range")
            return
        if LOGLEVEL > 0:
            logger.warning(
                f"Total traces downloaded: {len(all_traces)} in {pages} page(s) for LT range [{start_lt}, {end_lt}]")

        # Build a union of all blocks referenced by all traces
        union_blocks = set()
        for trace in all_traces:
            try:
                tx_map = trace.get("transactions", {})
                for _, txo in tx_map.items():
                    bref = txo.get("block_ref") or {}
                    wc = bref.get("workchain")
                    shard_hex = bref.get("shard")
                    seqno_b = bref.get("seqno")
                    if wc is not None and shard_hex is not None and seqno_b is not None:
                        try:
                            shard_int = int(shard_hex, 16) if isinstance(shard_hex, str) else int(shard_hex)
                            union_blocks.add((int(wc), shard_int, int(seqno_b)))
                        except Exception:
                            pass
            except Exception as e:
                logger.error(f"Failed to collect blocks for a trace: {e}")

        blocks_to_load_all = []
        for wc, shard_int, seq in sorted(union_blocks):
            try:
                blk = lc.lookup_block(BlockId(wc, shard_int, seq)).blk_id
                blocks_to_load_all.append(blk)
            except Exception as e:
                logger.error(f"Failed to lookup block ({wc},{hex(shard_int)},{seq}): {e}")

        if not blocks_to_load_all:
            logger.error("No blocks resolved to load for the collected traces")
            return

        # Scan all required blocks ONCE and collect raw data
        outq = Queue()
        raw_proc = collect_raw
        scanner = BlockScanner(
            lcparams=lcparams,
            start_from=None,
            load_to=None,
            nproc=int(os.getenv("NPROC", 10)),
            loglevel=LOGLEVEL,
            chunk_size=int(os.getenv("CHUNK_SIZE", 2)),
            tx_chunk_size=int(os.getenv("TX_CHUNK_SIZE", 40000)),
            raw_process=raw_proc,
            out_queue=outq,
            only_mc_blocks=bool(os.getenv("ONLYMC_BLOCK", False)),
            parse_txs_over_ls=True,
            blocks_to_load=blocks_to_load_all
        )
        scanner.start()

        raw_chunks_all = []
        while not scanner.done:
            while True:
                try:
                    raw_chunk = outq.get_nowait()
                    raw_chunks_all.extend(raw_chunk)
                except QueueEmpty:
                    break
            sleep(1)
        # Drain remaining
        while True:
            try:
                raw_chunk = outq.get_nowait()
                raw_chunks_all.extend(raw_chunk)
            except QueueEmpty:
                break

        # Now run TraceOrderedRunner for each trace using the same raw_chunks_all (multiprocessing with spawn + tqdm)
        max_workers = int(os.getenv("NPROC", 10))
        aggregated_failed: list = []
        pbar2 = tqdm(total=len(all_traces), desc="Emulating traces", unit="trace", disable=False)
        try:
            ctx = get_context("spawn")
            c7_env = os.getenv("C7_REWRITE")
            args_list = [
                (idx, trace, raw_chunks_all, lcparams, LOGLEVEL, COLOR_SCHEMA, c7_env)
                for idx, trace in enumerate(all_traces)
            ]
            with ctx.Pool(processes=max_workers) as pool:
                for res in pool.imap_unordered(
                        _process_one_trace_worker,
                        args_list):
                    if res:
                        aggregated_failed.extend(res)
                    pbar2.update(1)

            # Persist failed traces once
            try:
                if aggregated_failed:
                    # Merge with existing file if present
                    existing = []
                    try:
                        with open("failed_traces.json", "r") as f:
                            existing = json.load(f)
                            if not isinstance(existing, list):
                                existing = []
                    except Exception:
                        existing = []
                    existing.extend(aggregated_failed)
                    with open("failed_traces.json", "w") as f:
                        json.dump(existing, f)
            except Exception as e:
                logger.error(f"Failed to update failed_traces.json: {e}")

        finally:
            try:
                pbar2.close()
            except Exception:
                pass

            # Compute and print final summary for TONCENTER_TRACES_BY_MASTERS
            try:
                total_success = 0
                total_warnings = 0
                total_unsuccess = 0
                total_new = 0
                total_missed = 0
                trace_success_count = 0
                trace_unsuccess_count = 0

                def _count_modes_tree(node):
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

                for entry in aggregated_failed or []:
                    emu = entry.get('emulated_trace') or {}
                    c = _count_modes_tree(emu)
                    total_success += c.get('success', 0)
                    total_warnings += c.get('warnings', 0)
                    total_unsuccess += c.get('unsuccess', 0)
                    total_new += c.get('new', 0)
                    total_missed += c.get('missed', 0)
                    # Add also transactions from original trace that are not presented in emulated tree
                    not_presented = entry.get('not_presented') or []
                    total_missed += len(not_presented)

                    # Per-trace success means: all nodes are 'success' (no warnings/errors/new/missed) and no not_presented
                    if emu and c.get('warnings', 0) == 0 and c.get('unsuccess', 0) == 0 and c.get('new',
                                                                                                  0) == 0 and c.get(
                            'missed', 0) == 0 and len(not_presented) == 0:
                        trace_success_count += 1
                    else:
                        trace_unsuccess_count += 1

                logger.warning(
                    f"Final emulator status: {total_success} success, {total_unsuccess} unsuccess, {total_warnings} warnings, {total_new} new, {total_missed} missed; traces: {trace_success_count} success, {trace_unsuccess_count} unsuccess")
            except Exception as e:
                logger.error(f"Failed to compute final status from traces emulation: {e}")

            # Done with multi-trace mode
            return

    outq = Queue()
    config_override = os.getenv("C7_REWRITE", None)
    if config_override is not None:
        config_override = json.loads(config_override)

    blocks_to_load = None
    TRACE_TXS = None
    TX_ORDER_LIST = None
    # Determine if we are in trace mode (by tx hash or msg hash)
    trace_query_provided = bool(
        (toncenter_tx_hash and toncenter_tx_hash.strip()) or (toncenter_msg_hash and toncenter_msg_hash.strip()))
    if trace_query_provided:
        # Prepare param for toncenter (it accepts hex as in examples). Keep as-is.
        param_key = "tx_hash" if (toncenter_tx_hash and toncenter_tx_hash.strip()) else "msg_hash"
        param_value = (toncenter_tx_hash or toncenter_msg_hash).strip()
        # Query toncenter traces
        url = f"{toncenter_api.rstrip('/')}/traces"
        params = {param_key: param_value, "include_actions": "false", "limit": 10, "offset": 0, "sort": "desc"}
        headers = None
        if toncenter_api_key:
            headers = {
                'X-API-Key': toncenter_api_key,
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to query toncenter traces: {e}")
            raise
        data = resp.json()
        traces = data.get("traces", []) if isinstance(data, dict) else []
        if not traces:
            logger.error(f"No traces found for given {param_key}")
            return
        trace = traces[0]
        tx_map = trace.get("transactions", {})
        tx_order = trace.get("transactions_order", [])
        # Collect whitelist of tx hashes (convert base64 -> hex)

        TRACE_TXS = set()
        TX_ORDER_LIST = [b64_to_hex(h).upper() for h in tx_order]
        for transaction_hash in TX_ORDER_LIST:
            TRACE_TXS.add(transaction_hash)
        # Collect only blocks that actually contain transactions from the trace (via block_ref)
        blocks = set()  # tuples (workchain, shard_int, seqno)
        for _, txo in tx_map.items():
            bref = txo.get("block_ref") or {}
            wc = bref.get("workchain")
            shard_hex = bref.get("shard")
            seqno = bref.get("seqno")
            if wc is not None and shard_hex is not None and seqno is not None:
                try:
                    shard_int = int(shard_hex, 16) if isinstance(shard_hex, str) else int(shard_hex)
                    blocks.add((int(wc), shard_int, int(seqno)))
                except Exception:
                    pass
        # Build blocks_to_load using lc.lookup_block to get BlockIdExt for only those blocks
        blocks_to_load = []
        for wc, shard_int, seq in sorted(blocks):
            try:
                blk = lc.lookup_block(BlockId(wc, shard_int, seq)).blk_id
                blocks_to_load.append(blk)
            except Exception as e:
                logger.error(f"Failed to lookup block ({wc},{hex(shard_int)},{seq}): {e}")
        # Ensure we only load specific blocks
        from_seqno = None
        to_seqno = None

    elif TXS_TO_PROCESS is not None:
        blocks_to_load = []
        known_hash = set()
        from_seqno = None
        to_seqno = None

        for tx in TXS_TO_PROCESS:
            if tx['root_hash'] not in known_hash:
                blocks_to_load.append(BlockIdExt(BlockId(workchain=tx['workchain'],
                                                         shard=tx['shard'],
                                                         seqno=tx['seqno']),
                                                 root_hash=tx['root_hash'],
                                                 file_hash=tx['file_hash']))
                known_hash.add(tx['root_hash'])

    raw_proc = process_blocks(config_override=config_override,
                              trace_whitelist=TRACE_TXS) if not trace_query_provided else collect_raw

    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=from_seqno,
        load_to=to_seqno,
        nproc=int(os.getenv("NPROC", 10)),
        loglevel=LOGLEVEL,
        chunk_size=int(os.getenv("CHUNK_SIZE", 2)),
        tx_chunk_size=int(os.getenv("TX_CHUNK_SIZE", 40000)),
        raw_process=raw_proc,
        out_queue=outq,
        only_mc_blocks=bool(os.getenv("ONLYMC_BLOCK", False)),
        parse_txs_over_ls=True if trace_query_provided else bool(os.getenv("PARSE_OVER_LS", False)),
        blocks_to_load=blocks_to_load
    )

    scanner.start()

    success = 0
    warnings = 0
    unsuccess = []

    if not trace_query_provided:
        while not scanner.done:
            tmp_s, tmp_u, tmp_w = process_result(outq)
            success += tmp_s
            warnings += tmp_w
            unsuccess.extend(tmp_u)
            sleep(1)

        # After done some data can be in queue
        tmp_s, tmp_u, tmp_w = process_result(outq)
        success += tmp_s
        warnings += tmp_w
        unsuccess.extend(tmp_u)
    else:
        # Trace mode: collect raw chunks first
        raw_chunks = []
        while not scanner.done:
            while True:
                try:
                    raw_chunk = outq.get_nowait()
                    raw_chunks.extend(raw_chunk)
                except QueueEmpty:
                    break
            sleep(1)
        # Drain remaining
        while True:
            try:
                raw_chunk = outq.get_nowait()
                raw_chunks.extend(raw_chunk)
            except QueueEmpty:
                break

        runner = TraceOrderedRunner(raw_chunks=raw_chunks,
                                    config_override=config_override,
                                    loglevel=LOGLEVEL,
                                    color_schema=COLOR_SCHEMA,
                                    tx_order_hex_upper=TX_ORDER_LIST or [],
                                    toncenter_tx_map=trace.get("trace"),
                                    toncenter_tx_details=trace.get("transactions", {}),
                                    lcparams=lcparams)
        out_list = runner.run(TX_ORDER_LIST or [])
        # In trace mode we do not aggregate per-tx failed_txs; all info is captured in failed_traces.json.
        # Persist failed traces
        try:
            if getattr(runner, 'failed_traces', None):
                with open("failed_traces.json", "w") as f:
                    json.dump(runner.failed_traces, f)
        except Exception as e:
            logger.error(f"Failed to write failed_traces.json: {e}")

        # Compute final status counters from emulated_trace tree modes
        try:
            trace_entries = getattr(runner, 'failed_traces', None) or []
            if trace_entries:
                emu = trace_entries[0].get('emulated_trace') or {}

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
                        for ch in n.get('children', []) or []:
                            dfs(ch)

                    dfs(emu)
                    return cnt

                c = _count_modes(emu)
                success = c.get('success', 0)
                warnings = c.get('warnings', 0)
                unsuccess = c.get('unsuccess', 0)
                new_cnt = c.get('new', 0)
                missed_cnt = c.get('missed', 0)
                # Add also transactions from original trace that are not presented in emulated tree
                not_presented = trace_entries[0].get('not_presented') or []
                missed_cnt += len(not_presented)
                logger.warning(
                    f"Final emulator status: {success} success, {unsuccess} unsuccess, {warnings} warnings, {new_cnt} new, {missed_cnt} missed")
                # Skip the default final log below by returning early
                return
        except Exception as e:
            logger.error(f"Failed to compute final status from trace emulation: {e}")

    logger.warning(f"Final emulator status: {success} success, {len(unsuccess)} unsuccess, {warnings} warnings")

    if len(unsuccess) > 0:
        cnt = Counter()
        for i in unsuccess:
            cnt[i['address']] += 1

        logger.error(f"Unique addreses errors: {len(cnt)}, most common: ")
        logger.error(cnt.most_common(5))

        with open("failed_txs.json", "w") as f:
            json.dump(unsuccess, f)


if __name__ == '__main__':
    main()
