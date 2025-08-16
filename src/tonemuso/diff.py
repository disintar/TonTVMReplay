
from deepdiff import DeepDiff
from tonpy.autogen.block import Transaction, ShardAccount
from loguru import logger
import json


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
    log = {
        'affected_paths': list(diff.affected_paths),
        'colors': {}
    }
    for path in diff.affected_paths:
        path = unpack_path(path)

        current_root = color_schema[root]
        for p in path:
            if p not in current_root:
                logger.warning(f"[COLOR_SCHEMA] {p} is not in list, full path: {path}, but affected")
                max_level = 'alarm'
                log['colors']['__'.join(path)] = 'alarm'
                break
            else:
                if isinstance(current_root[p], str):
                    level = current_root[p]
                    if level == 'warn':
                        if max_level != 'alarm':
                            max_level = 'warn'
                        log['colors']['__'.join(path)] = 'warn'
                    elif level == 'alarm':
                        max_level = 'alarm'
                        log['colors']['__'.join(path)] = 'alarm'
                    elif level == 'skip':
                        pass
                    else:
                        logger.warning(f"[COLOR_SCHEMA] {p} has invalid color level")
                    break
                else:
                    current_root = current_root[p]

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
