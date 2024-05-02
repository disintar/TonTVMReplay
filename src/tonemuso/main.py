# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import Optional

from tonpy.blockscanner.blockscanner import *
from tonpy import begin_cell
from collections import Counter
import json
import os
from tonemuso.diff import get_diff, get_colored_diff

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


@curry
def process_blocks(data, config_override: dict = None):
    out = []
    block, account_state, txs = data
    if LOGLEVEL > 3:
        logger.debug(f"Start process block TXs: {len(txs)}")

    config: VmDict = VmDict(32, False, block['key_block']['config'])

    if config_override is not None:
        for param in config_override:
            config.set(int(param), begin_cell().store_ref(Cell(config_override[param])).end_cell().begin_parse())

    if LOGLEVEL > 3:
        logger.debug(f"Init emulator")

    em = EmulatorExtern(os.getenv("EMULATOR_PATH"), config)
    em.set_rand_seed(block['rand_seed'])
    prev_block_data = [list(reversed(block['prev_block_data'][0])), block['prev_block_data'][1]]
    em.set_prev_blocks_info(prev_block_data)
    em.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    if LOGLEVEL > 3:
        logger.debug(f"Emulator init success")

    if TXS_WHITELIST is not None:
        process_this_chunk = False

        for tx in txs:
            if tx['tx'].get_hash() in TXS_WHITELIST:
                process_this_chunk = True
                break

        if not process_this_chunk:
            return []

    if LOGLEVEL > 4:
        txs = tqdm(txs, desc="Emulate accounts")

    for tx in txs:
        try:
            current_tx_cs = tx['tx'].begin_parse()
            lt = tx['lt']
            now = tx['now']
            is_tock = tx['is_tock']

            tmp = current_tx_cs.load_ref(as_cs=True)

            if tmp.load_bool():
                in_msg = tmp.load_ref()
            else:
                in_msg = None

            if in_msg is None:
                success = em.emulate_tick_tock_transaction(
                    account_state,
                    is_tock,
                    now,
                    lt
                )
            else:
                # Emulate
                success = em.emulate_transaction(
                    account_state,
                    in_msg,
                    now,
                    lt)

            go_as_success = True

            if TXS_WHITELIST is not None and tx['tx'].get_hash() not in TXS_WHITELIST:
                account_state = em.account.to_cell()
                continue

            if not success or em.transaction is None:
                tx1_tlb = Transaction()
                tx1_tlb = tx1_tlb.cell_unpack(tx['tx'], True).dump()
                go_as_success = False
                out.append({'mode': 'error', 'expected': tx['tx'].get_hash(), 'address': tx1_tlb['account_addr'],
                            'cant_emulate': True,
                            'fail_reason': "emulation_new_failed"})

            # Emulation transaction equal current transaction
            if go_as_success and em.transaction.get_hash() != tx['tx'].get_hash():
                diff, address = get_diff(tx['tx'], em.transaction.to_cell())

                if COLOR_SCHEMA is None:
                    diff = diff.to_dict()
                    go_as_success = False
                    out.append(
                        {'mode': 'error', 'diff': str(diff), 'address': f"{block['block_id'].id.workchain}:{address}",
                         'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash(),
                         'fail_reason': "hash_missmatch"})
                else:
                    max_level, log = get_colored_diff(diff, COLOR_SCHEMA)
                    address = f"{block['block_id'].id.workchain}:{address}"
                    if max_level == 'alarm':
                        go_as_success = False
                        out.append(
                            {'mode': 'error', 'diff': str(diff), 'address': address,
                             'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash(), "color_schema_log": log,
                             'fail_reason': "color_schema_alarm"})
                    elif max_level == 'warn':
                        go_as_success = False
                        logger.warning(
                            f"[COLOR_SCHEMA] Warning! tx: {tx['tx'].get_hash()}, address: {address}, color_schema_log: {log}")
                        out.append({'mode': 'warning'})

            # Update account state, go to next transaction
            account_state = em.account.to_cell()
            if go_as_success:
                out.append({'mode': 'success'})
        except Exception as e:
            logger.error(f"EMULATOR ERROR: Got {e} while emulating!")
            raise e
    return out


def process_result(outq):
    total_txs = []
    while not outq.empty():
        total_txs.append(outq.get())

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


def main():
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
        'timeout': os.getenv('LITESERVER_TIMEOUT', 5),
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

    outq = Queue()
    config_override = os.getenv("C7_REWRITE", None)
    if config_override is not None:
        config_override = json.loads(config_override)

    blocks_to_load = None

    if TXS_TO_PROCESS is not None:
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

    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=from_seqno,
        load_to=to_seqno,
        nproc=int(os.getenv("NPROC", 10)),
        loglevel=LOGLEVEL,
        chunk_size=int(os.getenv("CHUNK_SIZE", 2)),
        tx_chunk_size=int(os.getenv("TX_CHUNK_SIZE", 40000)),
        raw_process=process_blocks(config_override=config_override),
        out_queue=outq,
        only_mc_blocks=bool(os.getenv("ONLYMC_BLOCK", False)),
        parse_txs_over_ls=bool(os.getenv("PARSE_OVER_LS", False)),
        blocks_to_load=blocks_to_load
    )

    scanner.start()

    success = 0
    warnings = 0
    unsuccess = []

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
