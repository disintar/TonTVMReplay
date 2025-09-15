# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
from loguru import logger

from tonemuso.config import Config
from tonemuso.modes import multi_traces, single_trace, whitelist, liteserver_base











def main():
    cfg = Config.from_env()

    if cfg.toncenter_traces_by_masters and multi_traces is not None:
        logger.warning(f"Run type: multi-trace")
        multi_traces.run(cfg)
        return
    else:
        if (cfg.toncenter_tx_hash and cfg.toncenter_tx_hash.strip()) or (cfg.toncenter_msg_hash and cfg.toncenter_msg_hash.strip()):
            logger.warning(f"Run type: single-trace")
            single_trace.run(cfg)
            return
        elif cfg.txs_to_process and isinstance(cfg.txs_to_process, dict) and (cfg.txs_to_process.get('transactions')):
            logger.warning(f"Run type: whitelist")
            whitelist.run(cfg)
            return
        if liteserver_base is not None:
            logger.warning(f"Run type: liteserver-base")
            liteserver_base.run(cfg)
            return



if __name__ == '__main__':
    main()
