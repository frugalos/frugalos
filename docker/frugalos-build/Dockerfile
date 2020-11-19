##
## FrugalOSビルド用のベースイメージ
##
FROM centos:7.8.2003

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
## ビルド対象の git のブランチ/コミットIDを指定する
ARG git_ref=master
RUN git clone https://github.com/frugalos/frugalos.git && cd frugalos && git checkout $git_ref
RUN cd frugalos && cargo build --release --all
