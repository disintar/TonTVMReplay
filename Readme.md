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
10. `ONLYMC_BLOCK` [OPTIONAL] - Emulate only MC blocks, (false by default)
11. `EMUSO_LOGLEVEL` [OPTIONAL] - 0 - no logs, 1 - logs per chunks, 2 - tqdm logs (1 by default)