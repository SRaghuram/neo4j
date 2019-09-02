FROM ubuntu:19.04
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
RUN \
  echo 'tzdata tzdata/Areas select Europe' | debconf-set-selections && \
  echo 'tzdata tzdata/Zones/Europe select Berlin' | debconf-set-selections && \
  apt-get --quiet --quiet update && \
  apt-get --quiet --quiet --no-install-recommends install \
    ansible \
    awscli \
    curl \
    unzip \
    openssh-client \
    docker.io \
    jq && \
  curl https://releases.hashicorp.com/packer/1.4.3/packer_1.4.3_linux_amd64.zip -o /tmp/packer.zip && \
  unzip /tmp/packer.zip -d /usr/local/bin/ && \
  rm /tmp/packer.zip && \
  rm -rf /var/lib/apt/lists/*
LABEL team="benchmarking"
LABEL project="infrastructure"
