# Deribit L2 Collector

Standalone recorder of Deribit full-depth order books (`book.<instrument>.100ms`) and trades for
BTC and ETH (inverse and USDC-linear) and SOL (USDC-linear) options and futures. New
instruments are detected automatically. Output is hourly zstd-compressed JSONL, ready for
upload.

## Build

Linux / Raspberry Pi:

```
sudo apt install g++ cmake git libssl-dev libzstd-dev ca-certificates
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
```

Windows (x64 Developer PowerShell, OpenSSL + zstd e.g. from conda):

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DOPENSSL_ROOT_DIR=<conda>/Library
cmake --build build
```

Unit tests: add `-DDERIBIT_COLLECTOR_TESTS=ON`, then run `build/collector_tests`.

## Run

```
deribit_collector --out /mnt/usb/deribit-data
```

| Option | Default | Meaning |
|---|---|---|
| `--out DIR` | `data` | Output root |
| `--bases LIST` | `BTC,ETH,SOL` | Base currencies to record |
| `--interval I` | `100ms` | Book channel interval (`100ms` or `agg2`) |
| `--per-conn N` | `400` | Instruments per WebSocket connection |
| `--zstd N` | `3` | Compression level |
| `--refresh S` | `600` | Full instrument list refresh period (s) |
| `--flush-seconds S` | `1` | Flush to disk period; worst-case data loss on power cut |
| `--stats-path FILE` | `<out>/stats.json` | Health file; put on tmpfs (`/run`) to spare flash |
| `--no-trades` | | Do not record trades |
| `--test` | | Use test.deribit.com |
| `--cafile PATH` | system | CA bundle for TLS |

Stop with SIGINT/SIGTERM: open files are finished and renamed.

## Raspberry Pi deployment

1. Mount the USB drive as ext4 with `noatime` (e.g. `/mnt/usb`) in `/etc/fstab`.
2. `sudo deploy/install.sh /mnt/usb` — builds, installs to `/opt/deribit-collector`,
   creates user `collector`, installs and starts `deribit-collector.service`.
3. `sudo -u collector rclone config`, set `REMOTE=` in `/etc/default/deribit-upload`,
   then `sudo systemctl enable --now deribit-upload.timer`.
   Every hour at :15 finished files are moved to the remote and deleted locally.

Monitoring: `journalctl -u deribit-collector -f`, `cat /run/deribit-collector/stats.json`.
Alert on growing `gaps`, `write_errors`, `dropped`, or `books_valid < instruments` for long.

Volume: about 130 MB/hour compressed (~3 GB/day) for all instruments.

## Output

```
<out>/deribit/<group>/<YYYY-MM-DD>/<HH>.jsonl.zst
```

Groups: `btc`, `eth` (inverse), `btc_usdc`, `eth_usdc`, `sol_usdc` (linear), `trades_btc`,
`trades_eth`, `trades_usdc`. Hours are UTC. A file is written as `.part` and renamed when
complete; a restart within the same hour produces `HH_1.jsonl.zst`, etc.

Each line is a JSON object with `type`:

| type | Content |
|---|---|
| `header` | First line: version, venue, group, interval, hour start |
| `instrument` | `event` = `checkpoint` (file start), `added`, `removed`; `info` = full `get_instruments` record |
| `snapshot` | Book rebuilt by the collector at file start: `instrument`, `change_id`, `bids`/`asks` as `[price, amount]` |
| `session` | Connection `conn` reconnected (`epoch`); its books restart from new Deribit snapshots |
| `gap` | `change_id` discontinuity for `instrument`; a resubscribe was requested |
| `book` | Raw Deribit notification in `msg`, local receive time `recv_ns`, connection `conn` |
| `trades` | Raw Deribit trades notification, same envelope as `book` |

Note: `trades_usdc` contains trades for every USDC-settled Deribit instrument (the channel is
per settlement currency); filter by `instrument_name` when reading.

## Replay

For each instrument:

1. Start from the `snapshot` record (file start) or a raw `book` message with
   `msg.params.data.type == "snapshot"`.
2. Apply each following `book` message whose `prev_change_id` equals the current `change_id`.
   Levels are `[action, price, amount]`; `delete` removes the price level, otherwise set it.
3. On `gap`, `session` for the instrument's connection, or a `prev_change_id` mismatch, discard
   the book until the next snapshot.

Use `recv_ns` (local receive time) for latency-realistic replay, or `msg.params.data.timestamp`
(Deribit ms) for exchange time.
