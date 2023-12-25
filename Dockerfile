
# The original Dockerfile can be found here: https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark
# we made some slight adaptions for PG 14.5 and integrated more steps into the Dockerfile

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y apt-transport-https ca-certificates


RUN apt-get update && apt-get install -y \
    wget \
    git \
    gcc \
    build-essential \
    libreadline-dev \
    zlib1g-dev \
    bison \
    flex \
    patch \
    python3 \
    python3-pip


COPY . /home/jgmp
RUN chmod -R 777 /home/jgmp

RUN cd /root/ \
    && wget https://ftp.postgresql.org/pub/source/v14.5/postgresql-14.5.tar.gz \
    && tar xvf postgresql-14.5.tar.gz  \
    && rm postgresql-14.5.tar.gz \
    && cd postgresql-14.5 \
    && patch -s -p1 < /home/jgmp/end-to-end-cardest/benchmark.patch \
    && ./configure --prefix=/usr/local/pgsql/14.5 --enable-depend --enable-cassert --enable-debug CFLAGS="-ggdb -O0" \
    && make \
    && make install \
    && echo 'export PATH=/usr/local/pgsql/14.5/bin:$PATH' >> /root/.bashrc \
    && echo 'export LD_LIBRARY_PATH=/usr/local/pgsql/14.5/lib/:$LD_LIBRARY_PATH' >> /root/.bashrc


ENV PATH $PATH:/usr/local/pgsql/14.5/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/pgsql/14.5/lib/

RUN set -eux \
	&& groupadd -r postgres --gid=999 \
	&& useradd -r -g postgres --uid=999 --home-dir=/var/lib/pgsql/14.5/data --shell=/bin/bash postgres \
	&& mkdir -p /var/lib/pgsql/14.5/data \
	&& chown -R postgres:postgres /var/lib/pgsql/14.5/data \
  && echo 'postgres' > /var/lib/pgsql/14.5/passwd \
  && chmod -R 777 /var/lib/pgsql/14.5/passwd

RUN chmod a+x /home/jgmp/init_pg.sh

RUN cat /home/jgmp/requirements.txt | xargs -n 1 pip3 install

USER postgres
RUN /home/jgmp/init_pg.sh

CMD ["postgres", "-D", "/var/lib/pgsql/14.5/data"]