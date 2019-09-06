FROM "535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracel_jdk_8" AS JDKSETUP
RUN ln -s /usr/lib/jvm/java-8-oracle /usr/lib/jvm/oracle-jdk-8 && \
    rm -rf /var/cache/oracle-jdk8-installer/

FROM "535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:profile" AS PROFILERSETUP
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV JAVA_HOME=/usr/lib/jvm/java-8-oracle
COPY --from=0 /usr/lib/jvm/java-8-oracle /usr/lib/jvm/java-8-oracle

RUN chmod +x /tmp/benchmarks-build/install-profilers.sh && \
    /tmp/benchmarks-build/install-profilers.sh /usr/lib

