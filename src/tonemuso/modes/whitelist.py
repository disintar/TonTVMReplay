import json
from collections import Counter
from loguru import logger
from tonpy.blockscanner.blockscanner import *

from tonemuso.config import Config
from tonemuso.modes.common import process_blocks, process_result


def run(cfg: Config):
    txs_whitelist = None
    txs_list = None
    if cfg.txs_to_process and isinstance(cfg.txs_to_process, dict):
        txs_list = cfg.txs_to_process.get('transactions') or []
        txs_whitelist = set(t['hash'] for t in txs_list)

    lcparams = cfg.lcparams()
    lc = LiteClient(**lcparams)

    # Build blocks_to_load from tx list
    blocks_to_load = []
    known = set()
    for tx in txs_list or []:
        if tx['root_hash'] in known:
            continue
        blocks_to_load.append(BlockIdExt(BlockId(workchain=tx['workchain'], shard=tx['shard'], seqno=tx['seqno']),
                                         root_hash=tx['root_hash'], file_hash=tx['file_hash']))
        known.add(tx['root_hash'])

    outq = Queue()
    raw_proc = process_blocks(config_override=cfg.c7_rewrite, trace_whitelist=None, loglevel=cfg.loglevel,
                              color_schema=cfg.color_schema, emulator_path=cfg.emulator_path,
                              emulator_unchanged_path=cfg.emulator_unchanged_path, txs_whitelist=txs_whitelist)

    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=None,
        load_to=None,
        nproc=int(cfg.nproc),
        loglevel=cfg.loglevel,
        chunk_size=int(cfg.chunk_size),
        tx_chunk_size=int(cfg.tx_chunk_size),
        raw_process=raw_proc,
        out_queue=outq,
        only_mc_blocks=bool(cfg.only_mc_blocks),
        parse_txs_over_ls=bool(cfg.parse_over_ls),
        blocks_to_load=blocks_to_load
    )

    scanner.start()

    success = 0
    warnings = 0
    unsuccess = []

    while not scanner.done:
        tmp_s, tmp_u, tmp_w = process_result(outq, loglevel=cfg.loglevel)
        success += tmp_s
        warnings += tmp_w
        unsuccess.extend(tmp_u)
        sleep(1)

    tmp_s, tmp_u, tmp_w = process_result(outq, loglevel=cfg.loglevel)
    success += tmp_s
    warnings += tmp_w
    unsuccess.extend(tmp_u)

    logger.warning(f"Final emulator status: {success} success, {len(unsuccess)} unsuccess, {warnings} warnings")
    if unsuccess:
        cnt = Counter()
        for i in unsuccess:
            cnt[i['address']] += 1
        logger.error(f"Unique addreses errors: {len(cnt)}, most common: ")
        logger.error(cnt.most_common(5))
        with open("failed_txs.json", "w") as f:
            json.dump(unsuccess, f)
