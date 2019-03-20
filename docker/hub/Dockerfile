# STAGE-1: Build frugalos binary
FROM rust:1.33.0-slim

ARG FRUGALOS_VERSION

RUN apt update
RUN apt install -y gcc automake libtool git make patch curl

RUN cargo install frugalos --version $FRUGALOS_VERSION


# STAGE-2: Copy the built binary
FROM debian

COPY --from=0 /usr/local/cargo/bin/frugalos /bin/frugalos
ENTRYPOINT ["/bin/frugalos"]
CMD ["--help"]
