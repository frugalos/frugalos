FROM ringo/scientific:7.2

ENV FRUGALOS_DATA_DIR /var/lib/frugalos/
ENV RUST_BACKTRACE 1

RUN yum -y install hostname
COPY bootstrap.sh /usr/bin/
COPY join.sh /usr/bin/
COPY frugalos /usr/bin/
