FROM ubuntu:18.04
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
COPY bootstrap-worker.sh /work/bootstrap-worker.sh
ADD profilers/install-profilers.sh /tmp/benchmarks-build/install-profilers.sh
RUN \
  echo 'tzdata tzdata/Areas select Europe' | debconf-set-selections && \
  echo 'tzdata tzdata/Zones/Europe select Berlin' | debconf-set-selections && \
  apt-get --quiet --quiet update && \
  apt-get --quiet --quiet install \
    sysstat uuid-runtime \
    software-properties-common \
    linux-tools-generic git \
    build-essential curl shellcheck \
    awscli uuid-runtime && \
    chmod +x /work/bootstrap-worker.sh
LABEL name="Profilers"
