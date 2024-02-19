from tonpy.blockscanner.blockscanner import *
from deepdiff import DeepDiff
from collections import Counter
from toolz import curry
import json
import os


@curry
def process_blocks(chunk, emulator_link):
    out = []

    for data in chunk:
        block, account_state, txs = data
        config = block['key_block']['config']

        em = EmulatorExtern(emulator_link, config)

        em.set_rand_seed(block['rand_seed'])
        block['prev_block_data'][0] = list(reversed(block['prev_block_data']))
        em.set_prev_blocks_info(block['prev_block_data'])
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

            if not success:
                tx1_tlb = Transaction()
                tx1_tlb = tx1_tlb.cell_unpack(tx['tx'], True).dump()

                out.append({'success': False, 'expected': tx['tx'].get_hash(), 'address': tx1_tlb['account_addr'],
                            'cant_emulate': True})

            # Emulation transaction equal current transaction
            if em.transaction.get_hash() != tx['tx'].get_hash():
                diff, address = get_diff(tx['tx'], em.transaction.to_cell())

                out.append({'success': False, 'diff': diff, 'address': f"{block['block_id'].id.workchain}:{address}",
                            'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash()})

            # Update account state, go to next transaction
            account_state = em.account.to_cell()
        out.append({'success': True})

    return out


def get_diff(tx1, tx2):
    tx1_tlb = Transaction()
    tx1_tlb = tx1_tlb.cell_unpack(tx1, True).dump()

    tx2_tlb = Transaction()
    tx2_tlb = tx2_tlb.cell_unpack(tx2, True).dump()

    diff = DeepDiff(tx1_tlb, tx2_tlb).to_dict()

    address = tx1_tlb['account_addr']
    del tx1_tlb
    del tx2_tlb

    return diff, address


def main():
    f = process_blocks(emulator_link=os.getenv("EMULATOR_PATH"))

    server = {
        "ip": os.getenv("LITESERVER_SERVER"),
        "port": os.getenv("LITESERVER_PORT"),
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
    to_seqno = os.getenv("TO_SEQNO", latest_seqno)
    from_seqno = os.getenv("FROM_SEQNO", None)

    if from_seqno is None:
        from_seqno = to_seqno - os.getenv("TO_EMULATE_MC_BLOCKS", 10)

    outq = Queue()

    scanner = BlockScanner(
        lcparams=lcparams,
        start_from=from_seqno,
        load_to=to_seqno,
        nproc=os.getenv("NPROC", 10),
        loglevel=1,
        chunk_size=os.getenv("CHUNK_SIZE", 2),
        raw_process=f,
        out_queue=outq
    )

    scanner.start()

    success = 0
    unsuccess = []

    while not scanner.done:
        total_txs = []

        if not outq.empty():
            total_txs.append(outq.get())

        if len(total_txs) > 0:
            tmp_s = 0
            tmp_u = []
            for chunk in total_txs:
                for i in chunk:
                    if i['success']:
                        tmp_s += 1
                    else:
                        tmp_u.append(i)

            logger.warning(f"Emulator status: {tmp_s} success, {len(tmp_u)} unsuccess")

            if len(tmp_u) > 0:
                cnt = Counter()
                for i in tmp_u:
                    cnt[i['address']] += 1

                logger.error(f"Unique addreses errors: {len(cnt)}, most common: ")
                logger.error(cnt.most_common(5))

            success += tmp_s
            unsuccess.extend(tmp_u)

        sleep(1)

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
