FROM rust:1.88.0-alpine AS builder
WORKDIR /app
# Install build dependencies
RUN apk add --no-cache clang musl-dev
COPY . .
RUN cargo build --release -p replicators

FROM alpine:latest
WORKDIR /app
# Install runtime dependencies
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/replicators replicator
ENTRYPOINT ["./replicator"]
