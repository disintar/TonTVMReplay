# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0

from tonpy.blockscanner.blockscanner import *
from deepdiff import DeepDiff
from collections import Counter
import json
import os

LOGLEVEL = int(os.getenv("EMUSO_LOGLEVEL", 1))


def get_diff(tx1, tx2):
    tx1_tlb = Transaction()
    tx1_tlb = tx1_tlb.cell_unpack(tx1, True).dump()

    tx2_tlb = Transaction()
    tx2_tlb = tx2_tlb.cell_unpack(tx2, True).dump()

    diff = DeepDiff(tx1_tlb, tx2_tlb).to_json()

    address = tx1_tlb['account_addr']
    del tx1_tlb
    del tx2_tlb

    return diff, address


def process_blocks(data):
    out = []
    block, account_state, txs = data
    config = block['key_block']['config']

    em = EmulatorExtern(os.getenv("EMULATOR_PATH"), config)

    em.set_rand_seed(block['rand_seed'])
    block['prev_block_data'][0] = list(reversed(block['prev_block_data']))
    try:
        print(len(block['prev_block_data']), len(block['prev_block_data'][0]))
        em.set_prev_blocks_info(block['prev_block_data'])
    except Exception as e:
        print(e, block['prev_block_data'])
        raise ValueError(e)

    em.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    for tx in txs:
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
        if not success:
            tx1_tlb = Transaction()
            tx1_tlb = tx1_tlb.cell_unpack(tx['tx'], True).dump()
            go_as_success = False
            out.append({'success': False, 'expected': tx['tx'].get_hash(), 'address': tx1_tlb['account_addr'],
                        'cant_emulate': True})

        # Emulation transaction equal current transaction
        if em.transaction.get_hash() != tx['tx'].get_hash():
            diff, address = get_diff(tx['tx'], em.transaction.to_cell())
            go_as_success = False
            out.append({'success': False, 'diff': diff, 'address': f"{block['block_id'].id.workchain}:{address}",
                        'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash()})

        # Update account state, go to next transaction
        account_state = em.account.to_cell()
        if go_as_success:
            out.append({'success': True})

    return out


def process_result(outq):
    total_txs = []
    while not outq.empty():
        total_txs.append(outq.get())

    tmp_s = 0
    tmp_u = []
    if len(total_txs) > 0:
        for chunk in total_txs:
            for i in chunk:
                if i['success']:
                    tmp_s += 1
                else:
                    tmp_u.append(i)

        if LOGLEVEL > 1:
            logger.warning(f"Emulator status: {tmp_s} success, {len(tmp_u)} unsuccess")

        if len(tmp_u) > 0 and LOGLEVEL > 1:
            cnt = Counter()
            for i in tmp_u:
                cnt[i['address']] += 1

            logger.error(f"Unique addreses errors: {len(cnt)}, most common: ")
            logger.error(cnt.most_common(5))

    return tmp_s, tmp_u


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
        'timeout': 1,
        'num_try': 3000,
        'threads': 1
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

    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=from_seqno,
        load_to=to_seqno,
        nproc=int(os.getenv("NPROC", 10)),
        loglevel=LOGLEVEL,
        chunk_size=int(os.getenv("CHUNK_SIZE", 2)),
        tx_chunk_size=int(os.getenv("TX_CHUNK_SIZE", 2)),
        raw_process=process_blocks,
        out_queue=outq,
        only_mc_blocks=bool(os.getenv("ONLYMC_BLOCK", False)),
        parse_txs_over_ls=bool(os.getenv("PARSE_OVER_LS", False))
    )

    scanner.start()

    success = 0
    unsuccess = []

    while not scanner.done:
        tmp_s, tmp_u = process_result(outq)
        success += tmp_s
        unsuccess.extend(tmp_u)
        sleep(1)

    # After done some data can be in queue
    tmp_s, tmp_u = process_result(outq)
    success += tmp_s
    unsuccess.extend(tmp_u)

    logger.warning(f"Final emulator status: {success} success, {len(unsuccess)} unsuccess")

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
