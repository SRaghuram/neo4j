FROM "065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-oraclejdk-8:0.0.1"
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV FLAMEGRAPH_DIR=usr/lib/flameGraph
ENV JFR_FLAMEGRAPH_DIR=usr/lib/jfr-flamegraph
ENV ASYNC_PROFILER_DIR=usr/lib/async-profiler

COPY bootstrap-worker.sh /work/bootstrap-worker.sh
# add AWS CLI, so we can fetch worker bootstrap
RUN chmod +x /work/bootstrap-worker.sh && \
	echo 'tzdata tzdata/Areas select Europe' | debconf-set-selections && \
  	echo 'tzdata tzdata/Zones/Europe select Berlin' | debconf-set-selections && \
    add-apt-repository --remove ppa:webupd8team/java && \
  	apt-get --quiet --quiet update && \
	apt-get --quiet --quiet install awscli uuid-runtime && \
	rm -rf /var/cache/oracle-jdk8-installer/
