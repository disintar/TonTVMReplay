# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List


@dataclass
class Config:
    # Logging / output
    loglevel: int = 1
    color_schema_path: Optional[str] = None
    color_schema: Optional[Dict[str, Any]] = None

    # Emulators
    emulator_path: Optional[str] = None
    emulator_unchanged_path: Optional[str] = None
    c7_rewrite_raw: Optional[str] = None
    c7_rewrite: Optional[Dict[str, Any]] = None

    # Input filters
    txs_to_process_path: Optional[str] = None
    txs_to_process: Optional[Dict[str, Any]] = None

    # Liteserver
    liteserver_ip: Optional[int] = None
    liteserver_port: Optional[int] = None
    liteserver_pubkey: Optional[str] = None
    liteserver_timeout: float = 5.0

    to_seqno: Optional[int] = None
    from_seqno: Optional[int] = None
    to_emulate_mc_blocks: int = 10

    only_mc_blocks: bool = False
    parse_over_ls: bool = False

    # Performance
    nproc: int = 10
    chunk_size: int = 2
    tx_chunk_size: int = 40000

    # Toncenter / traces
    toncenter_api: str = "https://toncenter.com/api/v3"
    toncenter_api_key: Optional[str] = None
    toncenter_tx_hash: Optional[str] = None
    toncenter_msg_hash: Optional[str] = None
    toncenter_traces_by_masters: bool = False

    def liteclient_server(self) -> Dict[str, Any]:
        return {
            "ip": int(self.liteserver_ip) if self.liteserver_ip is not None else 0,
            "port": int(self.liteserver_port) if self.liteserver_port is not None else 0,
            "id": {"@type": "pub.ed25519", "key": self.liteserver_pubkey or ""},
        }

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls()
        # Logging
        cfg.loglevel = int(os.getenv("EMUSO_LOGLEVEL", 1))
        cspath = os.getenv("COLOR_SCHEMA_PATH", "").strip()
        cfg.color_schema_path = cspath or None
        if cfg.color_schema_path:
            try:
                with open(cfg.color_schema_path, "r") as f:
                    cfg.color_schema = json.load(f)
            except Exception:
                cfg.color_schema = None
        else:
            cfg.color_schema = None

        # Emulators
        cfg.emulator_path = os.getenv("EMULATOR_PATH")
        cfg.emulator_unchanged_path = os.getenv("EMULATOR_UNCHANGED_PATH")
        cfg.c7_rewrite_raw = os.getenv("C7_REWRITE")
        try:
            cfg.c7_rewrite = json.loads(cfg.c7_rewrite_raw) if cfg.c7_rewrite_raw else None
        except Exception:
            cfg.c7_rewrite = None

        # Input filters
        cfg.txs_to_process_path = os.getenv("TXS_TO_PROCESS_PATH")
        if cfg.txs_to_process_path:
            try:
                with open(cfg.txs_to_process_path, "r") as f:
                    cfg.txs_to_process = json.load(f)
            except Exception:
                cfg.txs_to_process = None

        # Liteserver
        ls_ip = os.getenv("LITESERVER_SERVER")
        cfg.liteserver_ip = int(ls_ip) if ls_ip is not None else None
        ls_port = os.getenv("LITESERVER_PORT")
        cfg.liteserver_port = int(ls_port) if ls_port is not None else None
        cfg.liteserver_pubkey = os.getenv("LITESERVER_PUBKEY")
        cfg.liteserver_timeout = float(os.getenv("LITESERVER_TIMEOUT", 5))
        # seqno
        to_seq = os.getenv("TO_SEQNO")
        cfg.to_seqno = int(to_seq) if to_seq is not None else None
        from_seq = os.getenv("FROM_SEQNO")
        cfg.from_seqno = int(from_seq) if from_seq is not None else None
        cfg.to_emulate_mc_blocks = int(os.getenv("TO_EMULATE_MC_BLOCKS", 10))

        cfg.only_mc_blocks = bool(os.getenv("ONLYMC_BLOCK", False))
        cfg.parse_over_ls = bool(os.getenv("PARSE_OVER_LS", False))

        # Performance
        cfg.nproc = int(os.getenv("NPROC", 10))
        cfg.chunk_size = int(os.getenv("CHUNK_SIZE", 2))
        cfg.tx_chunk_size = int(os.getenv("TX_CHUNK_SIZE", 40000))

        # Toncenter
        cfg.toncenter_api = os.getenv("TONCENTER_API", cfg.toncenter_api)
        cfg.toncenter_api_key = os.getenv("TONCENTER_API_KEY")
        cfg.toncenter_tx_hash = os.getenv("TONCENTER_TX_HASH")
        cfg.toncenter_msg_hash = os.getenv("TONCENTER_MSG_HASH")
        cfg.toncenter_traces_by_masters = bool(os.getenv("TONCENTER_TRACES_BY_MASTERS"))
        return cfg

    def lcparams(self) -> Dict[str, Any]:
        return {
            'mode': 'roundrobin',
            'my_rr_servers': [self.liteclient_server()],
            'timeout': self.liteserver_timeout,
            'num_try': 3000,
            'threads': 1,
            'loglevel': max(self.loglevel - 3, 0)
        }
