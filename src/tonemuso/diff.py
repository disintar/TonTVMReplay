from deepdiff import DeepDiff
from tonpy.autogen.block import Transaction
from loguru import logger


class PathGetter:
    def __init__(self):
        self.path = []

    def __getitem__(self, item):
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
    path = eval(path)
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
                max_level = 'warn'
                log['keys']['__'.join(path)] = 'warn'
                break
            else:
                if isinstance(current_root[p], str):
                    level = current_root[p]
                    if level == 'warn':
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
