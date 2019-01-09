# STAGE-1: Build frugalos binary
FROM centos:7

ARG FRUGALOS_VERSION

##
## yumパッケージ群をインストール
##
RUN yum -y install gcc automake libtool vim git make patch

##
## Rustをインストール
##
RUN curl https://sh.rustup.rs -sSf > rustup.sh && sh rustup.sh -y
ENV PATH $PATH:/root/.cargo/bin

RUN cargo install frugalos --version $FRUGALOS_VERSION


# STAGE-2: Copy the built binary
FROM centos:7

COPY --from=0 /root/.cargo/bin/frugalos /bin/frugalos
ENTRYPOINT ["/bin/frugalos"]
CMD ["--help"]
