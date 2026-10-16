# syntax=docker/dockerfile:1.6

FROM lukemathwalker/cargo-chef:latest-rust-1.89 AS chef
# Устанавливаем cmake (для static) и librdkafka-dev + pkg-config (для dynamic)
RUN apt-get update && apt-get install -y cmake build-essential pkg-config librdkafka-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG BUILD_ARGS=""
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook ${BUILD_ARGS} --recipe-path recipe.json
COPY . .
RUN cargo build ${BUILD_ARGS} --locked

FROM debian:bookworm-slim AS runtime
ARG PROFILE="debug"
# librdkafka1 необходим для запуска при динамической линковке (dev)
RUN apt-get update && apt-get install -y ca-certificates libssl3 librdkafka1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/${PROFILE}/adaptive-microservice /usr/local/bin/app
RUN mkdir -p /app/config
EXPOSE 3000
CMD ["app"]