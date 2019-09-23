# global docker build arguments
ARG JVM_RELEASE=11.0.4_linux-x64_bin
ARG JAVA_HOME=/usr/lib/jvm/oracle-jdk-11

FROM ubuntu:18.04 as jdk
ARG JVM_RELEASE
ARG JAVA_HOME
COPY jdk-${JVM_RELEASE}.tar.gz /tmp/
RUN  \
  mkdir -p "${JAVA_HOME}" && \
  tar -C "${JAVA_HOME}" -xzvf /tmp/jdk-"${JVM_RELEASE}".tar.gz --strip 1

FROM ubuntu:18.04 as profilers
ARG JAVA_HOME
ENV JAVA_HOME=${JAVA_HOME}
ENV DEBIAN_FRONTEND noninteractive
ADD install-profilers.sh /tmp/benchmarks-build/install-profilers.sh
COPY --from=jdk ${JAVA_HOME}/ ${JAVA_HOME}/
RUN \
  echo 'tzdata tzdata/Areas select Europe' | debconf-set-selections && \
  echo 'tzdata tzdata/Zones/Europe select Berlin' | debconf-set-selections && \
  apt-get --quiet --quiet update && \
  apt-get --quiet --quiet --no-install-recommends install \
    git \
    build-essential \
    curl \
    ca-certificates && \
  chmod +x /tmp/benchmarks-build/install-profilers.sh && \
  /tmp/benchmarks-build/install-profilers.sh /usr/lib

FROM ubuntu:18.04 as buildessentials
ARG JAVA_HOME
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=${JAVA_HOME}
ENV PATH=${JAVA_HOME}/bin:${PATH}
ENV FLAMEGRAPH_DIR=/usr/lib/flamegraph/
ENV ASYNC_PROFILER_DIR=/usr/lib/async-profiler
ENV JFR_FLAMEGRAPH=/usr/lib/jfr-flamegraph
COPY setup-async-profiler.sh /etc/profile.d/setup-async-profiler.sh
COPY bootstrap-worker.sh /work/bootstrap-worker.sh
COPY --from=jdk ${JAVA_HOME}/ ${JAVA_HOME}
COPY --from=profilers /usr/lib/async-profiler/ /usr/lib/async-profiler/
COPY --from=profilers /usr/lib/flamegraph/ /usr/lib/flamegraph/
RUN \
  echo 'tzdata tzdata/Areas select Europe' | debconf-set-selections && \
  echo 'tzdata tzdata/Zones/Europe select Berlin' | debconf-set-selections && \
  apt-get --quiet --quiet update && \
  apt-get --quiet --quiet --no-install-recommends install \
    awscli \
    sysstat \
    uuid-runtime \
    linux-tools-generic \
    perl-modules && \
    chmod +x /work/bootstrap-worker.sh

LABEL name="Benchmark Oracle JDK 11"
