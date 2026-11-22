# Shopping Sessions: native Rust pipeline, std only (no crates, no network needed to build).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY fast/ /src/
# Scorers are Intel Xeon (Cascade Lake or newer): AVX2-level codegen on x86_64.
RUN if [ "$(uname -m)" = "x86_64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --release --offline && cp target/release/ss-fast /ss-fast

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /ss-fast /app/ss-fast
# At run time: read-only root, uid 1000, no network, /data read-only, /output and /scratch writable.
CMD ["/app/ss-fast"]
