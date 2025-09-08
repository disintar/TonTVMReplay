# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import List, Optional, Tuple, Dict, Any

from tonpy import Cell, VmDict, Address, CellSlice
from tonpy.tvm.not_native.emulator_extern import EmulatorExtern
from tonpy.autogen.block import Transaction, MessageAny
from loguru import logger
import os

from tonemuso.diff import get_diff, get_colored_diff, make_json_dumpable, get_shard_account_diff
from tonemuso.utils import hex_to_b64


def init_emulators(block: Dict[str, Any], config_override: Dict[str, Any] = None) -> Tuple[
    EmulatorExtern, Optional[EmulatorExtern]]:
    """
    Initialize primary (and optional secondary) emulators for a given block using
    the same logic as in main.process_blocks.
    Returns (em, em2_or_None)
    """
    # Base config from block key_block (no overrides)
    base_config: VmDict = VmDict(32, False, block['key_block']['config'])

    # Working config (may be overridden via C7_REWRITE)
    config: VmDict = VmDict(32, False, block['key_block']['config'])
    if config_override is not None:
        from tonpy import begin_cell
        for param in config_override:
            config.set(int(param), begin_cell().store_ref(Cell(config_override[param])).end_cell().begin_parse())

    em = EmulatorExtern(os.getenv("EMULATOR_PATH"), config)
    em.set_rand_seed(block['rand_seed'])

    prev_block_data = [list(reversed(block['prev_block_data'][1])),  # prev 16
                       block['prev_block_data'][2],  # key block
                       list(reversed(block['prev_block_data'][0]))]  # prev 16 by 100
    em.set_prev_blocks_info(prev_block_data)
    em.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    em2 = None
    unchanged_path = os.getenv("EMULATOR_UNCHANGED_PATH", "")
    if unchanged_path:
        em2 = EmulatorExtern(unchanged_path, base_config)
        em2.set_rand_seed(block['rand_seed'])
        em2.set_prev_blocks_info(prev_block_data)
        em2.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    return em, em2


def extract_message_info(tx: Cell):
    transaction_data = Transaction().fetch(tx).dump()
    answer = {
        'outmsg_cnt': transaction_data['outmsg_cnt'],
        'out_msgs': []
    }

    if transaction_data['outmsg_cnt'] > 0:
        for _, msg in VmDict(15, False, transaction_data['r1']['out_msgs']['root']):
            msg = msg.load_ref()
            msg_hash = msg.get_hash()
            message_parsed = MessageAny().fetch(msg.copy())
            is_internal = message_parsed.x.info.get_tag() == 0
            message_parsed = message_parsed.dump()

            if is_internal:
                send_body = message_parsed['x'].get('body', {}).get('value', None)

                opcode = None
                bodyhash = None
                if send_body is not None:
                    if isinstance(send_body, Cell):
                        send_body = send_body.begin_parse()
                    bodyhash = send_body.get_hash()
                    if send_body.bits >= 32:
                        opcode = hex(send_body.load_uint(32))

                dest = message_parsed['x']['info']['dest']
                created_lt = message_parsed['x']['info']['created_lt']
                dest = Address(f"{dest['workchain_id']}:{dest['address'].zfill(64)}")
                answer['out_msgs'].append({
                    'msg_hash': msg_hash,
                    'dest': dest,
                    'body': send_body,
                    'opcode': opcode,
                    'bodyhash': bodyhash,
                    'cell': msg,
                    'bounce': message_parsed['x']['info']['bounce'],
                    'bounced': message_parsed['x']['info']['bounced'],
                    'created_lt': created_lt
                })
    answer['out_msgs'] = list(sorted(answer['out_msgs'], key=lambda x: x['created_lt']))
    return answer


def emulate_tx_step(block: Dict[str, Any],
                    tx: Dict[str, Any],
                    em: EmulatorExtern,
                    em2: Optional[EmulatorExtern],
                    account_state_em1: Cell,
                    account_state_em2: Optional[Cell],
                    loglevel: int,
                    color_schema: Optional[Dict[str, Any]],
                    extract_out_msgs: bool = False,
                    force_state_check_without_emulation2=False) -> Tuple[List[Dict[str, Any]], Cell, Optional[Cell]]:
    """
    Emulate one transaction with provided emulators and current account states.
    Returns (out_entries, new_state_em1, new_state_em2)
    out_entries contains either [{'mode': 'success'}] or error/warn structures
    identical to main.process_blocks behavior.
    """
    out: List[Dict[str, Any]] = []

    current_tx_cs = tx['tx'].begin_parse()
    lt = tx['lt']
    now = tx['now']
    is_tock = tx['is_tock']

    if loglevel > 4:
        logger.debug(f"Start tx: {lt}, {now}, {is_tock}")

    tmp = current_tx_cs.load_ref(as_cs=True)

    if tmp.load_bool():
        in_msg = tmp.load_ref()
    else:
        in_msg = None

    if loglevel > 4:
        logger.debug(
            f"Run(em1): {account_state_em1.get_hash()} with in_msg {in_msg.get_hash() if in_msg is not None else None}")

    # Emulate with primary emulator
    if in_msg is None:
        success1 = em.emulate_tick_tock_transaction(
            account_state_em1,
            is_tock,
            now,
            lt
        )
    else:
        success1 = em.emulate_transaction(
            account_state_em1,
            in_msg,
            now,
            lt)

    # Emulate with secondary emulator if available (using its own state)
    success2 = None
    if em2 is not None:
        if loglevel > 4:
            logger.debug(
                f"Run(em2): {account_state_em2.get_hash()} with in_msg {in_msg.get_hash() if in_msg is not None else None}")
        if in_msg is None:
            success2 = em2.emulate_tick_tock_transaction(
                account_state_em2,
                is_tock,
                now,
                lt
            )
        else:
            success2 = em2.emulate_transaction(
                account_state_em2,
                in_msg,
                now,
                lt)

    if loglevel > 4:
        logger.debug(
            f"Run success(em1): {account_state_em1.get_hash()} -> {success1}, TX: {em.transaction}; "
            f"(em2): {account_state_em2.get_hash() if em2 else 'NA'} -> {success2 if em2 else 'NA'}, TX: {em2.transaction if em2 else 'NA'}")

    go_as_success = True

    # Primary emulator must succeed and produce a transaction
    if not success1 or em.transaction is None:
        if loglevel > 5:
            logger.debug(f"emulation_new_failed")

        tx1_tlb = Transaction()
        tx1_tlb = tx1_tlb.cell_unpack(tx['tx'], True).dump()
        go_as_success = False
        err = {'mode': 'error', 'expected': tx['tx'].get_hash(), 'address': tx1_tlb['account_addr'],
               'cant_emulate': True,
               'fail_reason': "emulation_new_failed"}
        # Attach em2 status if available
        if em2 is not None:
            err['unchanged_emulator_tx_hash'] = em2.transaction.get_hash() if (success2 and em2.transaction) else None
        out.append(err)

    # Emulation transaction equal current transaction
    if go_as_success and em.transaction.get_hash() != tx['tx'].get_hash():
        if loglevel > 5:
            logger.debug(f"hash_missmatch")

        diff, address = get_diff(tx['tx'], em.transaction.to_cell())

        # Always include secondary emulator info if available
        account_diff_dict = None
        account_color_level = None
        account_color_log = None
        unchanged_emulator_tx_hash = None
        sa_diff = None
        try:
            if em2 is not None:
                unchanged_emulator_tx_hash = em2.transaction.get_hash() if em2.transaction is not None else None
                sa_diff = get_shard_account_diff(em.account.to_cell(), em2.account.to_cell())
                account_diff_dict = {'data': make_json_dumpable(sa_diff.to_dict()),
                                     'account_emulator_tx_hash_match': unchanged_emulator_tx_hash == tx[
                                         'tx'].get_hash()}
                if color_schema is not None and 'account' in color_schema:
                    acc_level, acc_log = get_colored_diff(sa_diff, color_schema, root='account')
                    account_diff_dict['acc_level'] = acc_level
                    account_diff_dict['acc_log'] = acc_log

            elif force_state_check_without_emulation2:
                sa_diff = get_shard_account_diff(em.account.to_cell(), account_state_em2)
                account_diff_dict = {'data': make_json_dumpable(sa_diff.to_dict()),
                                     'account_emulator_tx_hash_match': unchanged_emulator_tx_hash == tx[
                                         'tx'].get_hash()}

                if color_schema is not None and 'account' in color_schema:
                    acc_level, acc_log = get_colored_diff(sa_diff, color_schema, root='account')
                    account_diff_dict['acc_level'] = acc_level
                    account_diff_dict['acc_log'] = acc_log


        except Exception as ee:
            logger.error(f"UNCHANGED EMULATOR ERROR: {ee}")

        if color_schema is None:
            diff_dict = diff.to_dict()
            go_as_success = False
            err_obj = {'mode': 'error', 'diff': make_json_dumpable(diff_dict),
                       'address': f"{block['block_id'].id.workchain}:{address}",
                       'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash(),
                       'fail_reason': "hash_missmatch"}
            if account_diff_dict is not None:
                err_obj['account_diff'] = account_diff_dict
            if unchanged_emulator_tx_hash is not None:
                err_obj['unchanged_emulator_tx_hash'] = unchanged_emulator_tx_hash
            out.append(err_obj)
        else:
            if loglevel > 5:
                logger.debug(f"Get color schema")

            # Evaluate transaction diff against color schema
            max_level, log = get_colored_diff(diff, color_schema)

            # Additionally, if we have account diff and color schema has 'account' section,
            # evaluate account diff and promote max_level accordingly. Attach account color log.
            if account_diff_dict is not None and 'account' in color_schema:
                try:
                    acc_level, acc_log = account_diff_dict['acc_level'], account_diff_dict['acc_log']
                    # Promote level: alarm > warn > skip
                    level_rank = {'alarm': 2, 'warn': 1, 'skip': 0}
                    if level_rank.get(acc_level, 0) > level_rank.get(max_level, 0):
                        max_level = acc_level
                    log = {**log, 'account': acc_log}
                except Exception as e:
                    logger.error(f"ACCOUNT COLOR SCHEMA ERROR: {e}")

            address = f"{block['block_id'].id.workchain}:{address}"

            if loglevel > 5:
                logger.debug(f"New max level: {max_level}")

            if max_level == 'alarm':
                logger.error(
                    f"[COLOR_SCHEMA] Alarm! tx: {hex_to_b64(tx['tx'].get_hash())}, address: {address}, color_schema_log: {log}")
                go_as_success = False
                diff_dict = diff.to_dict()
                err_obj = {'mode': 'error',
                           'diff': {
                               'transaction': make_json_dumpable(diff_dict),
                               'account': account_diff_dict.get('data', None) if isinstance(account_diff_dict,
                                                                                            dict) else None,
                           },
                           'address': address,
                           'expected': tx['tx'].get_hash(),
                           'got': em.transaction.get_hash(),
                           "color_schema_log": log,
                           'fail_reason': "color_schema_alarm"}
                if account_diff_dict is not None:
                    err_obj['account_diff'] = account_diff_dict
                if unchanged_emulator_tx_hash is not None:
                    err_obj['unchanged_emulator_tx_hash'] = unchanged_emulator_tx_hash
                out.append(err_obj)
            elif max_level == 'warn':
                go_as_success = False
                logger.warning(
                    f"[COLOR_SCHEMA] Warning! tx: {hex_to_b64(tx['tx'].get_hash())}, address: {address}, color_schema_log: {log}")
                warn_obj = {'mode': 'warning'}
                if account_diff_dict is not None:
                    warn_obj['account_diff'] = account_diff_dict
                if unchanged_emulator_tx_hash is not None:
                    warn_obj['unchanged_emulator_tx_hash'] = unchanged_emulator_tx_hash
                out.append(warn_obj)

    # Update account states for next transaction
    try:
        new_state_em1 = em.account.to_cell()
    except Exception as e:
        logger.error(f"EMULATOR ERROR, complete fail emulation of em1: {tx['tx'].get_hash()}")
        new_state_em1 = account_state_em1
        assert not go_as_success

    try:
        new_state_em2 = em2.account.to_cell() if em2 is not None else None
    except Exception as e:
        logger.error(f"EMULATOR ERROR, complete fail emulation of em1: {tx['tx'].get_hash()}")
        new_state_em2 = account_state_em2
        assert not go_as_success

    if go_as_success:
        out.append({'mode': 'success'})

    if extract_out_msgs and em.transaction is not None:
        out_msgs = extract_message_info(em.transaction.to_cell())
        return out, new_state_em1, new_state_em2, out_msgs
    else:
        return out, new_state_em1, new_state_em2


def emulate_tx_step_with_in_msg(block: Dict[str, Any],
                                tx: Dict[str, Any],
                                em: EmulatorExtern,
                                account_state_em1: Cell,
                                after_state_em2: Optional[Cell],
                                override_in_msg: Cell,
                                loglevel: int,
                                color_schema: Optional[Dict[str, Any]],
                                extract_out_msgs: bool = False) -> Tuple[List[Dict[str, Any]], Cell, Optional[Cell]]:
    """
    Emulate one transaction but force the provided override_in_msg as the incoming message.
    This performs the same checks and color-schema comparison against the original tx cell.
    Secondary emulator is not supported here (trace mode).
    Returns (out_entries, new_state_em1, None) and optionally out_msgs if extract_out_msgs.
    """
    out: List[Dict[str, Any]] = []

    lt = tx['lt']
    now = tx['now']

    if loglevel > 4:
        logger.debug(f"Start tx (override in_msg): {lt}, {now}")

    in_msg = override_in_msg

    if loglevel > 4:
        logger.debug(
            f"Run(em1-override): {account_state_em1.get_hash()} with in_msg {in_msg.get_hash() if in_msg is not None else None}")

    # Emulate with primary emulator using the override message
    success1 = em.emulate_transaction(account_state_em1, in_msg, now, lt)

    if loglevel > 4:
        logger.debug(
            f"Run success(em1-override): {account_state_em1.get_hash()} -> {success1}, TX: {em.transaction}")

    go_as_success = True

    if not success1 or em.transaction is None:
        if loglevel > 5:
            logger.debug(f"emulation_new_failed")

        tx1_tlb = Transaction()
        tx1_tlb = tx1_tlb.cell_unpack(tx['tx'], True).dump()
        go_as_success = False
        err = {'mode': 'error', 'expected': tx['tx'].get_hash(), 'address': tx1_tlb['account_addr'],
               'cant_emulate': True,
               'fail_reason': "emulation_new_failed"}
        out.append(err)

    # Compare produced transaction to original
    if go_as_success and em.transaction.get_hash() != tx['tx'].get_hash():
        if loglevel > 5:
            logger.debug(f"hash_missmatch")

        diff, address = get_diff(tx['tx'], em.transaction.to_cell())

        # Prepare account diff using provided after_state_em2 (post-state to compare against)
        account_diff_dict = None
        try:
            if after_state_em2 is not None:
                sa_diff = get_shard_account_diff(em.account.to_cell(), after_state_em2)
                account_diff_dict = {
                    'data': make_json_dumpable(sa_diff.to_dict()),
                    'account_emulator_tx_hash_match': em.transaction.get_hash() == tx['tx'].get_hash()
                }
                if color_schema is not None and 'account' in color_schema:
                    acc_level, acc_log = get_colored_diff(sa_diff, color_schema, root='account')
                    account_diff_dict['acc_level'] = acc_level
                    account_diff_dict['acc_log'] = acc_log
        except Exception as ee:
            logger.error(f"ACCOUNT DIFF ERROR: {ee}")

        if color_schema is None:
            diff_dict = diff.to_dict()
            go_as_success = False
            err_obj = {'mode': 'error', 'diff': {
                'transaction': make_json_dumpable(diff_dict),
                'account': account_diff_dict.get('data', None) if isinstance(account_diff_dict, dict) else None,
            },
                       'address': f"{block['block_id'].id.workchain}:{address}",
                       'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash(),
                       'fail_reason': "hash_missmatch"}
            if account_diff_dict is not None:
                err_obj['account_diff'] = account_diff_dict
            out.append(err_obj)
        else:
            if loglevel > 5:
                logger.debug(f"Get color schema")

            max_level, log = get_colored_diff(diff, color_schema)

            # If we have account diff and schema has 'account', evaluate and promote
            if account_diff_dict is not None and 'account' in color_schema:
                try:
                    acc_level, acc_log = account_diff_dict.get('acc_level'), account_diff_dict.get('acc_log')
                    level_rank = {'alarm': 2, 'warn': 1, 'skip': 0}
                    if level_rank.get(acc_level, 0) > level_rank.get(max_level, 0):
                        max_level = acc_level
                    log = {**log, 'account': acc_log}
                except Exception as e:
                    logger.error(f"ACCOUNT COLOR SCHEMA ERROR: {e}")

            address = f"{block['block_id'].id.workchain}:{address}"

            if loglevel > 5:
                logger.debug(f"New max level: {max_level}")

            if max_level == 'alarm':
                go_as_success = False
                diff_dict = diff.to_dict()
                err_obj = {'mode': 'error', 'diff': {
                    'transaction': make_json_dumpable(diff_dict),
                    'account': account_diff_dict.get('data', None) if isinstance(account_diff_dict, dict) else None,
                }, 'address': address,
                           'expected': tx['tx'].get_hash(), 'got': em.transaction.get_hash(),
                           "color_schema_log": log,
                           'fail_reason': "color_schema_alarm"}
                if account_diff_dict is not None:
                    err_obj['account_diff'] = account_diff_dict
                out.append(err_obj)
            elif max_level == 'warn':
                go_as_success = False
                logger.warning(
                    f"[COLOR_SCHEMA] Warning! tx: {hex_to_b64(tx['tx'].get_hash())}, address: {address}, color_schema_log: {log}")
                warn_obj = {'mode': 'warning'}
                if account_diff_dict is not None:
                    warn_obj['account_diff'] = account_diff_dict
                out.append(warn_obj)

    # Update state
    try:
        new_state_em1 = em.account.to_cell()
    except Exception:
        logger.error(f"EMULATOR ERROR, complete fail emulation of em1: {tx['tx'].get_hash()}")
        new_state_em1 = account_state_em1
        assert not go_as_success

    if go_as_success:
        out.append({'mode': 'success'})

    if extract_out_msgs and em.transaction is not None:
        out_msgs = extract_message_info(em.transaction.to_cell())
        return out, new_state_em1, None, out_msgs
    else:
        return out, new_state_em1, None
