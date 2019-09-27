FROM ringo/scientific:7.2

ENV FRUGALOS_DATA_DIR="/var/lib/frugalos" \
    FRUGALOS_CONFIG_DIR="/etc/frugalos" \
    RUST_BACKTRACE="1"

RUN yum -y install hostname
# /var/lib/frugalos は mount されるため別のディレクトリに設定を置く
RUN mkdir -p $FRUGALOS_CONFIG_DIR
COPY bootstrap.sh /usr/bin/
COPY join.sh /usr/bin/
COPY frugalos /usr/bin/
COPY frugalos.yml $FRUGALOS_CONFIG_DIR
