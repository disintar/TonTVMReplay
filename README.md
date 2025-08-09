## Envs

1. `LITESERVER_SERVER` - Liteserver IP in INT format
2. `LITESERVER_PORT` - Port for liteserver
3. `LITESERVER_PUBKEY` - Pubkey in base64 format
4. `EMULATOR_PATH` - Path to SO of emulator
5. `TO_SEQNO` [OPTIONAL] - Download data TO this MC seqno, if None - will download to latest block in LS
6. `FROM_SEQNO` [OPTIONAL] - Download data from this MC seqno, if None - use TO_EMULATE_MC_BLOCKS
7. `TO_EMULATE_MC_BLOCKS` [OPTIONAL] - Number of blocks from latest to emulate, 10 by default
8. `NPROC` [OPTIONAL] - Number of process to start (more process, more CPU & RAM load), 10 by default
9. `CHUNK_SIZE` [OPTIONAL] - Number of MC block to load and emulate by 1 iteration (2 by default)
10. `EMUSO_LOGLEVEL` [OPTIONAL] - 0 - disabled, 1 - per chunk, 2 - tqdm (1 by default)
11. `ONLYMC_BLOCK` [OPTIONAL] - Emulate only MC blocks (false by default)
12. `TX_CHUNK_SIZE` [OPTIONAL] - Num of TXs emulated by 1 iteration (40k is good, if <32gb ram - consider to use lower)
13. `C7_REWRITE` [OPTIONAL] - `{1: "base64 boc"}` json to override C7 params
14. `COLOR_SCHEMA_PATH` [OPTIONAL] - Path to JSON schema of TLB "colors" - warning/error/ignore diff checks on fields.
15. `TXS_TO_PROCESS_PATH` [OPTIONAL] - Path to JSON file with ordered TXs to check
16. `LITESERVER_TIMEOUT` [OPTIONAL] - Timeout per one LC call
17. `EMULATOR_UNCHANGED_PATH` [OPTIONAL] - Path to SO of emulator for ShardAccount diff compare
18. `TX_HASH` [OPTIONAL] - Transaction hash (hex or base64) to trace via Toncenter and emulate the involved blocks
19. `TONCENTER_API` [OPTIONAL] - Toncenter API base URL (default: https://toncenter.com/api/v3)

## Color schema

- "skip" - skip check for field
- "warn" - warn if field missmatch, but calc as success
- "alarm" - calc as unsuccessful if field missmatch

[Example of JSON](https://github.com/disintar/TonTVMReplay/blob/master/diff_colored.json)

## Trace run

Option A: Use toncenter traces by tx hash (new)

- Usage:
  - Set env `TX_HASH=<HEX_OR_BASE64_TX_HASH>`
  - Optionally set `TONCENTER_API=https://toncenter.com/api/v3` (default is already this)
  - Run: `tonemuso`
- The tool will:
  - Query Toncenter v3 traces for the given TX_HASH;
  - Load only blocks that contain transactions from the trace via liteserver;
  - Emulate transactions and produce diff like in blockscanner mode.

Option B: Provide explicit TX list JSON

For each TX need to provide (transaction) `hash`, `lt`, `workchain`, `shard`, `seqno`, `root_hash`, `file_hash`.

If you have TX of initial trace tx you can get all fields:

```
{
  transactions(
    trace_hash: "9CE993000E4D40F81FBE867972E712ECD6D55849D8E0FC1F54A2ACF657BD9315"
    page_size: 150
  ) {
    hash
    lt
    workchain
    shard
    seqno
    root_hash
    file_hash
  }
}
```

[Example of JSON](https://github.com/disintar/TonTVMReplay/blob/master/trace.json)
