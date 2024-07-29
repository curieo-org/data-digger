FROM rust:1.79 AS planner
WORKDIR app

RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rust:1.79 AS cacher
WORKDIR app
RUN cargo install cargo-chef
COPY --from=planner /app/recipe.json recipe.json
# Build our project dependencies, not our application!
RUN cargo chef cook --release --recipe-path recipe.json

FROM rust:1.79 AS builder

WORKDIR app
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN cargo build --release --bin ranking

FROM debian:bookworm-slim AS runtime
WORKDIR app
COPY sql sql
COPY config config
COPY ca-certificates ca-certificates
COPY --from=builder /app/target/release/ranking /usr/local/bin
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends openssl \
    # Clean up
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

RUN useradd -u 1000 ranking

USER ranking

ENTRYPOINT ["/usr/local/bin/ranking"]
