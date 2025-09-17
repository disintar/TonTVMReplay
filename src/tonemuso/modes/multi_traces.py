from tonemuso.toncenter_api import ToncenterAPI
from tonemuso.toncenter_models import TonTrace
from queue import Empty as QueueEmpty

from tonpy.blockscanner.blockscanner import *
from tonpy.autogen.block import BlockId, Block, BlockInfo

from tonemuso.config import Config
from tonemuso.modes.common import collect_raw, worker_init, process_one_trace_worker, build_preindex
from tonemuso.utils import count_modes_tree


def run(cfg: Config):
    lcparams = cfg.lcparams()
    lc = LiteClient(**lcparams)

    to_seqno = int(cfg.to_seqno or lc.get_masterchain_info_ext().last.id.seqno)
    from_seqno = cfg.from_seqno or (to_seqno - int(cfg.to_emulate_mc_blocks))

    # Resolve LTs
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

    api = ToncenterAPI(base_url=cfg.toncenter_api, api_key=cfg.toncenter_api_key, timeout=60)

    all_traces = []
    offset = 0
    limit = 1000
    downloaded = 0
    pages = 0
    pbar = tqdm(total=None, unit="tr", desc="Downloading traces", disable=False)

    num_run = 0
    while True:
        try:
            traces_page = api.get_traces_by_lt_range_typed(start_lt=start_lt, end_lt=end_lt, include_actions=False, limit=limit, offset=offset, sort='desc')
        except Exception as e:
            logger.error(f"Failed to query toncenter traces page offset={offset}: {e}")
            if num_run > 10:
                raise ValueError(f"Can't get traces from toncenter for given LT range")
            else:
                num_run += 1
                continue
        page_traces = traces_page or []
        if not page_traces:
            break
        all_traces.extend(page_traces)
        downloaded += len(page_traces)
        pages += 1
        pbar.update(len(page_traces))
        offset += limit
    pbar.close()

    logger.debug(
        f"Toncenter multi-trace fetch complete: total={len(all_traces)}, pages={pages}, downloaded={downloaded}, lt_range=[{start_lt},{end_lt}]")
    if not all_traces:
        logger.error("No traces returned from toncenter for given LT range")
        return

    # Union of blocks
    union_blocks = set()
    for trace in all_traces:
        try:
            for _, txd in trace.transactions.items():
                bref = txd.block_ref
                if bref is not None:
                    union_blocks.add((int(bref.workchain), int(bref.shard), int(bref.seqno)))

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

    # Scan
    outq = Queue()
    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=None,
        load_to=None,
        nproc=int(cfg.nproc),
        loglevel=cfg.loglevel,
        chunk_size=int(cfg.chunk_size),
        tx_chunk_size=int(cfg.tx_chunk_size),
        raw_process=collect_raw(config_override=cfg.c7_rewrite, loglevel=cfg.loglevel, emulator_path=cfg.emulator_path,
                                emulator_unchanged_path=cfg.emulator_unchanged_path, trace_tx_hashes_hex=set()),
        out_queue=outq,
        only_mc_blocks=bool(cfg.only_mc_blocks),
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
    while True:
        try:
            raw_chunk = outq.get_nowait()
            raw_chunks_all.extend(raw_chunk)
        except QueueEmpty:
            break

    preindex = build_preindex(raw_chunks_all)

    # Run per-trace in pool
    max_workers = int(cfg.nproc)
    aggregated_failed: list = []
    pbar2 = tqdm(total=len(all_traces), desc="Emulating traces", unit="trace", disable=False)

    ctx = get_context("spawn")
    c7_env = cfg.c7_rewrite_raw
    args_iter = list(enumerate(all_traces))
    with ctx.Pool(processes=max_workers, initializer=worker_init,
                  initargs=(preindex, lcparams, cfg.loglevel, cfg.color_schema, c7_env, cfg.emulator_path, cfg.emulator_unchanged_path)) as pool:
        for res in pool.imap_unordered(process_one_trace_worker, args_iter):
            if res:
                aggregated_failed.extend(res)
            pbar2.update(1)

    if aggregated_failed:
        import json
        with open("failed_traces.json", "w") as f:
            json.dump(aggregated_failed, f)

    total_success = total_warnings = total_unsuccess = total_new = total_missed = 0
    trace_success_count = trace_warning_count = trace_unsuccess_count = 0
    for entry in aggregated_failed or []:
        emu = entry.get('emulated_trace') or {}
        c = count_modes_tree(emu)
        total_success += c.get('success', 0)
        total_warnings += c.get('warnings', 0)
        total_unsuccess += c.get('unsuccess', 0)
        total_new += c.get('new', 0)
        total_missed += c.get('missed', 0)
        not_presented = entry.get('not_presented') or []
        total_missed += len(not_presented)
        fstatus = (entry.get('final_status') or '').lower()
        if fstatus == 'success':
            trace_success_count += 1
        elif fstatus == 'warning' or fstatus == 'warnings':
            trace_warning_count += 1
        else:
            trace_unsuccess_count += 1
    logger.warning(
        f"Final emulator status: {total_success} success, {total_unsuccess} unsuccess, {total_warnings} warnings, {total_new} new, {total_missed} missed; traces: {trace_success_count} success, {trace_warning_count} warnings, {trace_unsuccess_count} unsuccess")
