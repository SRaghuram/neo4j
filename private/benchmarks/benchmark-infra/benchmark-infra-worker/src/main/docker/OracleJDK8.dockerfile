FROM ubuntu:18.04 as jdk
ARG JVM_VERSION=8u221
COPY jdk-$JVM_VERSION-linux-x64.tar.gz /tmp/
RUN  \
  mkdir -p /usr/lib/jvm/oracle-jdk-8 && \
  tar -C /usr/lib/jvm/oracle-jdk-8 -xzvf /tmp/jdk-$JVM_VERSION-linux-x64.tar.gz --strip 1

FROM ubuntu:18.04 as profilers
ENV JAVA_HOME=/usr/lib/jvm/oracle-jdk-8
ENV DEBIAN_FRONTEND noninteractive
ADD install-profilers.sh /tmp/benchmarks-build/install-profilers.sh
ADD setup-async-profiler.sh /etc/profile.d/setup-async-profiler.sh
COPY --from=jdk /usr/lib/jvm/oracle-jdk-8/ /usr/lib/jvm/oracle-jdk-8/
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
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/oracle-jdk-8
ENV PATH=${JAVA_HOME}/bin:${PATH}
ENV FLAMEGRAPH_DIR=/usr/lib/flamegraph/
ENV JFR_FLAMEGRAPH_DIR=/usr/lib/jfr-flamegraph
ENV ASYNC_PROFILER_DIR=/usr/lib/async-profiler
COPY bootstrap-worker.sh /work/bootstrap-worker.sh
COPY --from=jdk /usr/lib/jvm/oracle-jdk-8/ /usr/lib/jvm/oracle-jdk-8/
COPY --from=profilers /usr/lib/async-profiler/ /usr/lib/async-profiler/
COPY --from=profilers /usr/lib/flamegraph/ /usr/lib/flamegraph/
COPY --from=profilers /usr/lib/jfr-flamegraph/ /usr/lib/jfr-flamegraph/
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
