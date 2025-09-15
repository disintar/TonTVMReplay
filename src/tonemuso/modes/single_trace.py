from loguru import logger
from queue import Empty as QueueEmpty
from tonemuso.toncenter_api import ToncenterAPI
import json
from tonemuso.toncenter_models import TonTrace
from tonpy.blockscanner.blockscanner import *
from tonpy.autogen.block import BlockId

from tonemuso.config import Config
from tonemuso.trace_runner import TraceOrderedRunner
from tonemuso.utils import b64_to_hex, count_modes_tree
from tonemuso.modes.common import collect_raw


def _short_b64(b64: str, head: int = 6, tail: int = 6) -> str:
    try:
        if not isinstance(b64, str):
            return str(b64)
        if len(b64) <= head + tail + 1:
            return b64
        return f"{b64[:head]}…{b64[-tail:]}"
    except Exception:
        return str(b64)


def _addr_suffix(raw_addr: str, n: int = 8) -> str:
    try:
        if not isinstance(raw_addr, str) or ':' not in raw_addr:
            return "?"
        _, hexpart = raw_addr.split(':', 1)
        hexpart = hexpart.strip()
        if len(hexpart) <= n:
            return hexpart
        return hexpart[-n:]
    except Exception:
        return "?"


def pp_node(node: dict, prefix: str = "", is_last: bool = True):
    tx_b64 = node.get('tx_hash')
    acct_sfx = _addr_suffix(node.get('destination'))
    order = node.get('emulation_order')
    depth = node.get('depth')
    buffer_emulated_count = node.get('buffer_emulated_count')

    order_str = f"[#{order}]" if isinstance(order, int) else ""
    depth_str = f" d={depth}" if isinstance(depth, int) else ""
    buffer_emulated_count_str = f" buffer={buffer_emulated_count}"

    connector = "└─" if is_last else "├─"
    if prefix == "" and is_last:
        logger.warning(
            f"Emulated root: {order_str}{depth_str}{buffer_emulated_count_str} {_short_b64(tx_b64)} @ {acct_sfx}")
    else:
        logger.warning(f"{prefix}{connector} {order_str}{depth_str}{buffer_emulated_count_str} {_short_b64(tx_b64)} @ {acct_sfx}")
    children = node.get('children') or []
    new_prefix = prefix + ("   " if is_last else "│  ")
    for i, ch in enumerate(children):
        last = (i == len(children) - 1)
        in_msg_b64 = ch.get('in_msg_hash')
        if isinstance(in_msg_b64, str) and len(in_msg_b64) > 0:
            edge_label = _short_b64(in_msg_b64, 4, 4)
            logger.warning(f"{new_prefix}{'┌' if last else '┌'}─ edge {edge_label}")
        pp_node(ch, new_prefix, last)


def pretty_print_initial_graph(trace: TonTrace) -> None:
    try:
        root = getattr(trace, 'trace', None)
        if root is None:
            return
        txs = trace.transactions or {}

        def account_of(tx_b64: str) -> str:
            txd = txs.get(tx_b64)
            if txd is None or txd.in_msg is None:
                return "?"
            return _addr_suffix(txd.in_msg.destination_raw)

        def pp_node(node, prefix: str = "", is_last: bool = True):
            tx_b64 = getattr(node, 'tx_hash_b64', None)
            acct_sfx = account_of(tx_b64) if isinstance(tx_b64, str) else "?"
            connector = "└─" if is_last else "├─"
            # Root line has no prefix connector
            if prefix == "" and is_last:
                logger.warning(f"Trace root: {_short_b64(tx_b64)} @ {acct_sfx}")
            else:
                logger.warning(f"{prefix}{connector} {_short_b64(tx_b64)} @ {acct_sfx}")
            children = getattr(node, 'children', []) or []
            new_prefix = prefix + ("   " if is_last else "│  ")
            for i, ch in enumerate(children):
                last = (i == len(children) - 1)
                # Optionally annotate the edge with child's in_msg hash tail
                in_msg_b64 = getattr(ch, 'in_msg_hash_b64', None)
                if isinstance(in_msg_b64, str) and len(in_msg_b64) > 0:
                    edge_label = _short_b64(in_msg_b64, 4, 4)
                    logger.warning(f"{new_prefix}{'┌' if last else '┌'}─ edge {edge_label}")
                pp_node(ch, new_prefix, last)

        pp_node(root, "", False)
    except Exception:
        # Do not break the flow if pretty-print fails
        pass


def run(cfg: Config):
    lcparams = cfg.lcparams()
    lc = LiteClient(**lcparams)

    toncenter_tx_hash = cfg.toncenter_tx_hash
    toncenter_msg_hash = cfg.toncenter_msg_hash
    toncenter_api_key = cfg.toncenter_api_key
    toncenter_api = cfg.toncenter_api
    api = ToncenterAPI(base_url=toncenter_api, api_key=toncenter_api_key, timeout=20)

    outq = Queue()
    # Query toncenter for trace
    param_key = "tx_hash" if (toncenter_tx_hash and toncenter_tx_hash.strip()) else "msg_hash"
    param_value = (toncenter_tx_hash or toncenter_msg_hash).strip()

    traces_typed = api.get_traces_by_hash_typed(tx_hash=toncenter_tx_hash, msg_hash=toncenter_msg_hash,
                                                include_actions=False, limit=10, offset=0, sort='desc')
    logger.debug(f"Toncenter single-trace query via {param_key}={param_value}: fetched {len(traces_typed)} trace(s)")
    if not traces_typed:
        logger.error(f"No traces found for given {param_key}")
        return
    trace: TonTrace = traces_typed[0]
    tx_map = trace.transactions
    tx_order = trace.transactions_order_b64
    try:
        first_tx = (tx_order[0] if tx_order else None)
        logger.debug(
            f"Using first trace: transactions={len(tx_map)}, order_len={len(tx_order)}, first_order_b64={first_tx}")
    except Exception:
        pass

    TX_ORDER_LIST = [b64_to_hex(h).upper() for h in tx_order]

    # Pretty print the initial original trace graph (accounts and child links)
    pretty_print_initial_graph(trace)

    # Collect blocks to load
    blocks = set()
    for _, txd in tx_map.items():
        bref = txd.block_ref
        if bref is not None:
            try:
                blocks.add((int(bref.workchain), int(bref.shard), int(bref.seqno)))
            except Exception:
                pass

    blocks_to_load = []
    for wc, shard_int, seq in sorted(blocks):
        try:
            blk = lc.lookup_block(BlockId(wc, shard_int, seq)).blk_id
            blocks_to_load.append(blk)
        except Exception as e:
            logger.error(f"Failed to lookup block ({wc},{hex(shard_int)},{seq}): {e}")

    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=None,
        load_to=None,
        nproc=int(cfg.nproc),
        loglevel=cfg.loglevel,
        chunk_size=int(cfg.chunk_size),
        tx_chunk_size=int(cfg.tx_chunk_size),
        raw_process=collect_raw(config_override=cfg.c7_rewrite, loglevel=cfg.loglevel, emulator_path=cfg.emulator_path,
                                emulator_unchanged_path=cfg.emulator_unchanged_path,
                                trace_tx_hashes_hex=set(TX_ORDER_LIST)),
        out_queue=outq,
        only_mc_blocks=bool(cfg.only_mc_blocks),
        parse_txs_over_ls=True,
        blocks_to_load=blocks_to_load
    )
    scanner.start()

    raw_chunks = []
    while not scanner.done:
        while True:
            try:
                raw_chunk = outq.get_nowait()
                raw_chunks.extend(raw_chunk)
            except QueueEmpty:
                break
        sleep(1)
    while True:
        try:
            raw_chunk = outq.get_nowait()
            raw_chunks.extend(raw_chunk)
        except QueueEmpty:
            break

    runner = TraceOrderedRunner(raw_chunks=raw_chunks,
                                config_override=cfg.c7_rewrite,
                                loglevel=cfg.loglevel,
                                color_schema=cfg.color_schema,
                                tx_order_hex_upper=TX_ORDER_LIST or [],
                                toncenter_trace=trace,
                                lcparams=lcparams,
                                emulator_path=cfg.emulator_path,
                                emulator_unchanged_path=cfg.emulator_unchanged_path)
    runner.run(TX_ORDER_LIST or [])

    # Pretty print the emulated tree with emulation_order annotations

    pp_node(runner.emulated_trace_root or {}, "", False)

    import json
    if getattr(runner, 'failed_traces', None):
        with open("failed_traces.json", "w") as f:
            json.dump(runner.failed_traces, f)

    trace_entries = getattr(runner, 'failed_traces', None) or []
    if trace_entries:
        emu = trace_entries[0].get('emulated_trace') or {}
        c = count_modes_tree(emu)
        success = c.get('success', 0)
        warnings = c.get('warnings', 0)
        unsuccess = c.get('unsuccess', 0)
        new_cnt = c.get('new', 0)
        missed_cnt = c.get('missed', 0)
        not_presented = trace_entries[0].get('not_presented') or []
        missed_cnt += len(not_presented)
        status = (trace_entries[0].get('final_status') or '').lower()
        t_succ = 1 if status == 'success' else 0
        t_warn = 1 if status == 'warning' or status == 'warnings' else 0
        t_uns = 1 if status not in ('success', 'warning', 'warnings') else 0
        logger.warning(
            f"Final emulator status: {success} success, {unsuccess} unsuccess, {warnings} warnings, {new_cnt} new, {missed_cnt} missed; traces: {t_succ} success, {t_warn} warnings, {t_uns} unsuccess")
        return
