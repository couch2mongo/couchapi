FROM public.ecr.aws/docker/library/rust:1.73.0-bookworm AS builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

#############

FROM public.ecr.aws/docker/library/debian:bookworm-slim AS runtime

WORKDIR /usr/src/app

COPY --from=builder /usr/src/app/target/release/couchapi .
COPY views views
COPY updates updates

RUN apt-get update && apt-get install -y ca-certificates

CMD ["./couchapi"]
