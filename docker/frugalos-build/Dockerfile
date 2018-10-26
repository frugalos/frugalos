##
## FrugalOSビルド用のベースイメージ
##
FROM centos:7

##
## yumパッケージ群をインストール
##
RUN yum -y install gcc automake libtool vim git make patch

##
## Rustをインストール
##
RUN curl https://sh.rustup.rs -sSf > rustup.sh && sh rustup.sh -y
ENV PATH $PATH:/root/.cargo/bin

##
## Frugalosをビルド
##
RUN git clone https://github.com/frugalos/frugalos.git
RUN cd frugalos && cargo build --release --all
