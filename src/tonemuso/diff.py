from deepdiff import DeepDiff
from tonpy.autogen.block import Transaction, ShardAccount
from loguru import logger
import json
import re

def make_json_dumpable(obj):
    """
    Convert a diff object to a JSON-dumpable dictionary.
    Cell and CellSlice objects are converted using their .to_boc() method.
    Other non-JSON-serializable objects are converted to strings.
    """
    if isinstance(obj, type):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: make_json_dumpable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_dumpable(item) for item in obj]
    elif hasattr(obj, 'to_boc'):
        return obj.to_boc()
    elif isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    else:
        try:
            json.dumps(obj)
            return obj
        except (TypeError, OverflowError):
            return str(obj)


class PathGetter:
    def __init__(self):
        self.path = []

    def __getitem__(self, item):
        self.path.append(item)
        return self

    def __getattr__(self, item):
        self.path.append(item)
        return self

    def get_path(self):
        return self.path


def get_diff(tx1, tx2):
    tx1_tlb = Transaction()
    tx1_tlb = tx1_tlb.cell_unpack(tx1, True).dump()

    tx2_tlb = Transaction()
    tx2_tlb = tx2_tlb.cell_unpack(tx2, True).dump()

    diff = DeepDiff(tx1_tlb, tx2_tlb)

    address = tx1_tlb['account_addr']
    del tx1_tlb
    del tx2_tlb

    return diff, address


def unpack_path(path):
    root = PathGetter()
    try:
        path = eval(path)
    except Exception as e:
        logger.error(f"Incorrect path: {path}")
        raise e
    return path.get_path()


def get_colored_diff(diff, color_schema, root='transaction'):
    max_level = 'skip'
    # Prepare quick lookup for numeric value changes by affected path string
    diff_dict = diff.to_dict() if hasattr(diff, 'to_dict') else {}
    values_changed = diff_dict.get('values_changed', {}) if isinstance(diff_dict, dict) else {}

    log = {
        'affected_paths': list(diff.affected_paths),
        'colors': {}
    }
    for affected_path_str in diff.affected_paths:
        path_list = unpack_path(affected_path_str)

        if root not in color_schema:
            raise ValueError(f"{root} is not in color schema")

        current_root = color_schema[root]
        traversed_ok = True
        for i, p in enumerate(path_list):
            if p not in current_root:
                logger.warning(f"[COLOR_SCHEMA: {root}] {p} is not in list, full path: {path_list}, but affected")
                max_level = 'alarm'
                log['colors']['__'.join(path_list)] = 'alarm'
                traversed_ok = False
                break
            node = current_root[p]
            # Leaf processing
            if isinstance(node, str):
                level = node
                if level == 'warn':
                    if max_level != 'alarm':
                        max_level = 'warn'
                    log['colors']['__'.join(path_list)] = 'warn'
                elif level == 'alarm':
                    max_level = 'alarm'
                    log['colors']['__'.join(path_list)] = 'alarm'
                elif level == 'skip':
                    log['colors']['__'.join(path_list)] = 'skip'
                else:
                    logger.warning(f"[COLOR_SCHEMA] {p} has invalid color level")
                break
            elif isinstance(node, dict) and ('warn_if' in node):
                # Evaluate warn_if rule only for numeric value changes
                change = values_changed.get(affected_path_str)
                try:
                    old_v = change.get('old_value') if isinstance(change, dict) else None
                    new_v = change.get('new_value') if isinstance(change, dict) else None
                except Exception:
                    old_v, new_v = None, None
                applied = False
                diff_v = None
                # Precompute diff if both are ints
                if isinstance(old_v, int) and isinstance(new_v, int):
                    diff_v = new_v - old_v
                else:
                    logger.warning(f"[COLOR_SCHEMA] {p} has non-numeric value change, but warn_if rule is defined")
                    log['colors']['__'.join(path_list)] = 'alarm'
                    break

                rule = node.get('warn_if', '')
                # Conservative parser: supports patterns like "diff > 3 else skip", "abs_diff >= 5 else skip" or "new_value > 100 else warn"
                m = re.match(r"\s*(diff|abs_diff|new_value)\s*(==|!=|>=|<=|>|<)\s*(-?\d+)\s*else\s*(skip|warn|alarm)\s*$", str(rule))
                if m:
                    lhs, op, num_s, else_action = m.groups()
                    num = int(num_s)
                    # Determine comparable value based on lhs
                    can_compare = False
                    val = None
                    if lhs == 'diff' and isinstance(old_v, int) and isinstance(new_v, int):
                        val = diff_v
                        can_compare = True
                    elif lhs == 'abs_diff' and isinstance(old_v, int) and isinstance(new_v, int):
                        val = abs(diff_v)
                        can_compare = True
                    elif lhs == 'new_value' and isinstance(new_v, int):
                        val = new_v
                        can_compare = True
                    if can_compare:
                        cond = False
                        if op == '==':
                            cond = (val == num)
                        elif op == '!=':
                            cond = (val != num)
                        elif op == '>':
                            cond = (val > num)
                        elif op == '<':
                            cond = (val < num)
                        elif op == '>=':
                            cond = (val >= num)
                        elif op == '<=':
                            cond = (val <= num)
                        else:
                            logger.warning(f"[COLOR_SCHEMA] {p} has invalid operator")
                            log['colors']['__'.join(path_list)] = 'alarm'
                            break
                        if cond:
                            # warn
                            if max_level != 'alarm':
                                max_level = 'warn'
                            log['colors']['__'.join(path_list)] = 'warn'
                        else:
                            # else action
                            if else_action == 'alarm':
                                max_level = 'alarm'
                                log['colors']['__'.join(path_list)] = 'alarm'
                            elif else_action == 'warn':
                                if max_level != 'alarm':
                                    max_level = 'warn'
                                log['colors']['__'.join(path_list)] = 'warn'
                            elif else_action == 'skip':
                                log['colors']['__'.join(path_list)] = 'skip'
                            else:
                                logger.warning(f"[COLOR_SCHEMA] {p} has invalid else action")
                                log['colors']['__'.join(path_list)] = 'alarm'
                                break
                        applied = True
                if not applied:
                    logger.warning(f"[COLOR_SCHEMA] {p} rule is not applied, but affected")
                    log['colors']['__'.join(path_list)] = 'alarm'
                    break
                break
            else:
                # Traverse deeper
                current_root = node
        # end for path components

    return max_level, log


def get_shard_account_diff(sa1_cell, sa2_cell):
    """
    Compare two ShardAccount cells using DeepDiff on their dumped structures.
    Returns a DeepDiff object. Address is not returned as ShardAccount dump may not include it in a uniform field.
    """
    sa1 = ShardAccount()
    sa1_dump = sa1.cell_unpack(sa1_cell, True).dump()

    sa2 = ShardAccount()
    sa2_dump = sa2.cell_unpack(sa2_cell, True).dump()

    diff = DeepDiff(sa1_dump, sa2_dump)
    return diff
