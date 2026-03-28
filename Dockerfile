FROM rust:1.85-bookworm AS builder

ARG PACKAGE
ARG BIN
ARG CARGO_FEATURES

WORKDIR /workspace

# Use the toolchain already baked into the base image instead of asking rustup
# to refresh the floating `1.85` channel during each container build.
ENV RUSTUP_TOOLCHAIN=1.85.1-x86_64-unknown-linux-gnu

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN test -n "${PACKAGE}" && test -n "${BIN}"
RUN if [ -n "${CARGO_FEATURES}" ]; then \
      cargo build --release -p "${PACKAGE}" --bin "${BIN}" --features "${CARGO_FEATURES}"; \
    else \
      cargo build --release -p "${PACKAGE}" --bin "${BIN}"; \
    fi

FROM debian:bookworm-slim

ARG BIN

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/${BIN} /usr/local/bin/service

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/service"]
