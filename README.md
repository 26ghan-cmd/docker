# Shopping Sessions: native Rust pipeline

A submission for the Databricks Shopping Sessions track. One self-contained Rust binary,
standard library only (no crates), so the Docker build needs no network beyond pulling the base images.

## Layout

| Path | What |
| --- | --- |
| `Dockerfile` | Builds `fast/` and runs it; reads `/data`, writes `/output/{user_daily,campaign_daily}/*.parquet` |
| `fast/` | The pipeline (`src/main.rs`) and a minimal Parquet writer (`src/parquet.rs`) |
| `ref/ref.py` | A slow, literal Python implementation of `rules.md`, used as the oracle |
| `fixtures/` | The starter's 36 edge-case fixtures |
| `tools/` | Fixture runner, a strict Parquet reader, dev-answer check, reference comparison |

## How it works

1. **Parse** (parallel, ~2 MB chunks, SIMD delimiter scan): events are validated as they are read
   and bucketed by `event_id` hash; clicks are validated; link lines are bucketed by ID hash.
2. **People**: link IDs are interned per bucket, joined with a lock-free union-find, and every
   person is named by the byte-wise smallest ID.
3. **Dedup** (per bucket): the copy with the smallest `ingest_ts` wins, before validation, so a bad
   earliest copy drops the event. Events and clicks are then mapped to people and re-bucketed by person.
4. **Sessions** (per person bucket): group by person, sort by `ts`, split on gaps over 30 minutes,
   credit each session to the latest click within 7 days (ties: smallest `click_id`), count assists,
   aggregate per day.
5. **Write** Parquet (uncompressed, PLAIN, `day` as `DATE`).

## Testing

```sh
cd fast && cargo build --release && cd ..
python3 tools/run_fixtures.py fixtures -- fast/target/release/ss-fast     # 36/36
fast/target/release/ss-fast dev out && python3 tools/check_dev.py out    # dev set vs published answer
tools/cmp_ref.sh 2 300000 80000                                          # any seed vs the reference (needs `generate`)
```

`SS_TIMING=1` prints per-phase timings; `NCPU` sets the thread count.
