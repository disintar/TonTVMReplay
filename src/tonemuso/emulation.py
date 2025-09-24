# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from typing import List, Optional, Tuple, Dict, Any

from tonpy import Cell, VmDict, Address
from tonpy.tvm.not_native.emulator_extern import EmulatorExtern
from tonpy.autogen.block import Transaction, MessageAny, ShardAccount
from loguru import logger

from tonemuso.diff import get_diff, get_colored_diff, make_json_dumpable, get_shard_account_diff


def init_emulators(block: Dict[str, Any], config_override: Dict[str, Any], emulator_path: str,
                   emulator_unchanged_path: str) -> Tuple[EmulatorExtern, EmulatorExtern]:
    """
    Initialize primary and secondary emulators for a given block using
    the same logic as in main.process_blocks.
    Returns (em, em2)
    """
    # Base config from block key_block (no overrides)
    base_config: VmDict = VmDict(32, False, block['key_block']['config'])

    # Working config (may be overridden via C7_REWRITE)
    config: VmDict = VmDict(32, False, block['key_block']['config'])
    if config_override is not None:
        from tonpy import begin_cell
        for param in config_override:
            config.set(int(param), begin_cell().store_ref(Cell(config_override[param])).end_cell().begin_parse())

    em = EmulatorExtern(emulator_path, config)
    em.set_rand_seed(block['rand_seed'])

    prev_block_data = [list(reversed(block['prev_block_data'][1])),  # prev 16
                       block['prev_block_data'][2],  # key block
                       list(reversed(block['prev_block_data'][0]))]  # prev 16 by 100
    em.set_prev_blocks_info(prev_block_data)
    em.set_libs(VmDict(256, False, cell_root=Cell(block['libs'])))

    em2 = EmulatorExtern(emulator_unchanged_path, base_config)
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


class TxStepEmulator:
    """
    Unified transaction emulation stepper. Replaces emulate_tx_step and emulate_tx_step_with_in_msg.
    Now accepts emulators and their account states in __init__.

    Usage:
      step = TxStepEmulator(block=block, loglevel=loglevel, color_schema=color_schema, em=em, account_state_em1=account_state_em1, em2=em2, account_state_em2=account_state_em2)
      out, st1, st2, out_msgs = step.emulate(tx, extract_out_msgs=True)
      # or with override in_msg (em2 is still executed and used for comparison):
      step = TxStepEmulator(block=block, loglevel=loglevel, color_schema=color_schema, em=em, account_state_em1=account_state_em1, em2=em2, account_state_em2=account_state_em2)
      out, st1, st2, out_msgs = step.emulate(tx, override_in_msg=cell, extract_out_msgs=True)

    Returns:
      Tuple[out_entries, new_state_em1, new_state_em2|None, out_msgs|None]
    """

    def __init__(self,
                 block: Dict[str, Any],
                 loglevel: int,
                 color_schema: Optional[Dict[str, Any]],
                 em: EmulatorExtern,
                 account_state_em1: Cell,
                 em2: EmulatorExtern,
                 account_state_em2: Cell,
                 use_boc_for_diff: bool = False) -> None:
        self.block = block
        self.loglevel = loglevel
        self.color_schema = color_schema
        # Emulators and states
        self.em: EmulatorExtern = em
        self.em2: EmulatorExtern = em2
        self.state1: Cell = account_state_em1
        self.state2: Cell = account_state_em2
        # Diff behavior flag: when True, compare BOCs instead of hashes
        self.use_boc_for_diff: bool = bool(use_boc_for_diff)

    # ---- Small helpers to keep emulate() readable ----
    def _prepare_in_msg(self, tx: Dict[str, Any], override_in_msg: Optional[Cell]) -> Tuple[
        Optional[Cell], int, int, bool]:
        lt = tx['lt']
        now = tx['now']
        is_tock = tx['is_tock'] if override_in_msg is None else False
        if override_in_msg is not None:
            in_msg = override_in_msg
        else:
            current_tx_cs = tx['tx'].begin_parse()
            tmp = current_tx_cs.load_ref(as_cs=True)
            if tmp.load_bool():
                in_msg = tmp.load_ref()
            else:
                in_msg = None
        return in_msg, lt, now, is_tock

    def _run_primary(self, in_msg: Optional[Cell], now: int, lt: int, is_tock: bool) -> bool:
        assert self.em is not None and self.state1 is not None
        if in_msg is None:
            return self.em.emulate_tick_tock_transaction(self.state1, is_tock, now, lt)
        return self.em.emulate_transaction(self.state1, in_msg, now, lt)

    def _run_secondary(self, in_msg: Optional[Cell], now: int, lt: int, is_tock: bool) -> bool:
        assert self.em2 is not None and self.state2 is not None
        if in_msg is None:
            return self.em2.emulate_tick_tock_transaction(self.state2, is_tock, now, lt)
        return self.em2.emulate_transaction(self.state2, in_msg, now, lt)


    def _extract_account_code_hash(self):
        try:
            sa = self.em2.account.to_cell()
            sa_o = ShardAccount()
            return  sa_o.cell_unpack(sa, True).account.storage.state.x.code.value.get_hash()
        except Exception as e:
            return 'UNKNOWN'


    def _compare_and_color(self, tx: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]]]:
        out: List[Dict[str, Any]] = []
        go_as_success = True
        account_code_hash = self._extract_account_code_hash()
        if self.em is None or self.em.transaction is None or self.em2 is None or self.em2.transaction is None:
            tx1_tlb = Transaction().cell_unpack(tx['tx'], True).dump()
            go_as_success = False
            err = {'mode': 'error', 'expected': tx['tx'].get_hash(), 'address': tx1_tlb['account_addr'],
                   'cant_emulate': True, 'fail_reason': 'emulation_new_failed', 'account_code_hash': account_code_hash}
            if self.em2 is not None and self.em2.transaction is not None:
                err['unchanged_emulator_tx_hash'] = self.em2.transaction.get_hash()
            else:
                err['cant_emulate_em2'] = True

            out.append(err)
            return go_as_success, out

        if self.em.transaction.get_hash() != tx['tx'].get_hash():
            diff, address = get_diff(tx['tx'], self.em.transaction.to_cell(), to_boc=self.use_boc_for_diff)

            unchanged_emulator_tx_hash = self.em2.transaction.get_hash()
            sa_diff = get_shard_account_diff(self.em2.account.to_cell(), self.em.account.to_cell())

            account_diff_dict: Optional[Dict[str, Any]] = None
            if sa_diff is not None:
                account_diff_dict = {
                    'data': make_json_dumpable(sa_diff.to_dict()),
                    'account_emulator_tx_hash_match': (unchanged_emulator_tx_hash == tx['tx'].get_hash())
                }
                if self.color_schema is not None and 'account' in self.color_schema:
                    acc_level, acc_log = get_colored_diff(sa_diff, self.color_schema, root='account')
                    account_diff_dict['acc_level'] = acc_level
                    account_diff_dict['acc_log'] = acc_log

            if self.color_schema is None:
                diff_dict = diff.to_dict()
                go_as_success = False
                err_obj: Dict[str, Any] = {'mode': 'error', 'diff': {
                    'transaction': make_json_dumpable(diff_dict),
                    'account': account_diff_dict.get('data', None) if isinstance(account_diff_dict, dict) else None,
                },
                                           'address': f"{self.block['block_id'].id.workchain}:{address}",
                                           'expected': tx['tx'].get_hash(), 'got': self.em.transaction.get_hash(),
                                           'fail_reason': 'hash_missmatch',
                                           'account_code_hash': account_code_hash}
                if account_diff_dict is not None:
                    err_obj['account_diff'] = account_diff_dict
                if unchanged_emulator_tx_hash is not None:
                    err_obj['unchanged_emulator_tx_hash'] = unchanged_emulator_tx_hash
                out.append(err_obj)
            else:
                max_level, log = get_colored_diff(diff, self.color_schema)
                if account_diff_dict is not None and 'account' in self.color_schema:
                    try:
                        acc_level, acc_log = account_diff_dict.get('acc_level'), account_diff_dict.get('acc_log')
                        level_rank = {'alarm': 2, 'warn': 1, 'skip': 0}
                        if level_rank.get(acc_level, 0) > level_rank.get(max_level, 0):
                            max_level = acc_level
                        log = {**log, 'account': acc_log}
                    except Exception as e:
                        logger.error(f"ACCOUNT COLOR SCHEMA ERROR: {e}")

                address_str = f"{self.block['block_id'].id.workchain}:{address}"
                if max_level == 'alarm':
                    go_as_success = False
                    diff_dict = diff.to_dict()
                    err_obj = {'mode': 'error', 'diff': {
                        'transaction': make_json_dumpable(diff_dict),
                        'account': account_diff_dict.get('data', None) if isinstance(account_diff_dict, dict) else None,
                    }, 'address': address_str,
                               'expected': tx['tx'].get_hash(), 'got': self.em.transaction.get_hash(),
                               'color_schema_log': log,
                               'fail_reason': 'color_schema_alarm',
                               'account_code_hash': account_code_hash}
                    if account_diff_dict is not None:
                        err_obj['account_diff'] = account_diff_dict
                    if unchanged_emulator_tx_hash is not None:
                        err_obj['unchanged_emulator_tx_hash'] = unchanged_emulator_tx_hash
                    out.append(err_obj)
                elif max_level == 'warn':
                    go_as_success = False
                    # Include diff for warning the same way as for error to aid diagnostics
                    diff_dict = diff.to_dict()
                    warn_obj: Dict[str, Any] = {
                        'mode': 'warning',
                        'diff': {
                            'transaction': make_json_dumpable(diff_dict),
                            'account': account_diff_dict.get('data', None) if isinstance(account_diff_dict, dict) else None,
                        },
                        'account_code_hash': account_code_hash,
                    }
                    if account_diff_dict is not None:
                        warn_obj['account_diff'] = account_diff_dict
                    if unchanged_emulator_tx_hash is not None:
                        warn_obj['unchanged_emulator_tx_hash'] = unchanged_emulator_tx_hash
                    out.append(warn_obj)
        return go_as_success, out

    def _maybe_extract_out_msgs(self, extract_out_msgs: bool) -> Optional[Dict[str, Any]]:
        if extract_out_msgs and self.em is not None and self.em.transaction is not None:
            return extract_message_info(self.em.transaction.to_cell())
        return None

    def emulate(
            self,
            tx: Dict[str, Any],
            override_in_msg: Optional[Cell] = None,
            extract_out_msgs: bool = False
    ) -> Tuple[List[Dict[str, Any]], Cell, Optional[Cell], Optional[Dict[str, Any]]]:
        # Prepare
        in_msg, lt, now, is_tock = self._prepare_in_msg(tx, override_in_msg)

        if self.loglevel > 4:
            if override_in_msg is None:
                logger.debug(f"Start tx: {lt}, {now}, {is_tock}")
            else:
                logger.debug(f"Start tx (override in_msg): {lt}, {now}")

        # Primary
        success1 = self._run_primary(in_msg, now, lt, is_tock)
        # Secondary (always)
        success2 = self._run_secondary(in_msg, now, lt, is_tock)

        if self.loglevel > 4:
            logger.debug(
                f"Run success(em1{('-override' if override_in_msg is not None else '')}): {self.state1.get_hash() if self.state1 is not None else None} -> {success1}, TX: {self.em.transaction if self.em is not None else None}; (em2): {self.state2.get_hash() if self.state2 is not None else None} -> {success2}, TX: {self.em2.transaction if self.em2 is not None else None}")

        # Compare/hash/color (always using em2)
        go_as_success, out = self._compare_and_color(tx)

        # Finalize states
        new_state_em1, new_state_em2 = self.em.account.to_cell(), self.em2.account.to_cell()
        # Update internal states for subsequent calls when this instance is reused
        self.state1 = new_state_em1
        self.state2 = new_state_em2

        if go_as_success:
            out.append({'mode': 'success', 'account_code_hash': self._extract_account_code_hash()})

        out_msgs = self._maybe_extract_out_msgs(extract_out_msgs)
        return out, new_state_em1, new_state_em2, out_msgs
