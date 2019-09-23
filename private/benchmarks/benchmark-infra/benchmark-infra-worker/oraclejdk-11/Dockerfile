FROM "535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracel_jdk_11" AS JDKSETUP
RUN ln -s /usr/lib/jvm/java-11-oracle /usr/lib/jvm/oracle-jdk-11 && \
    rm -rf /var/cache/oracle-jdk11-installer/

FROM "535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:profile" AS PROFILERSETUP
ENV JAVA_HOME=/usr/lib/jvm/java-11-oracle
COPY --from=0 /usr/lib/jvm/java-11-oracle /usr/lib/jvm/java-11-oracle

RUN chmod +x /tmp/benchmarks-build/install-profilers.sh && \
    /tmp/benchmarks-build/install-profilers.sh /usr/lib

