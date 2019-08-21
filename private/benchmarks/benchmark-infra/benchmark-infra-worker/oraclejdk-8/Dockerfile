FROM "065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-oraclejdk-8:0.0.1" AS JDKSETUP
RUN ln -s /usr/lib/jvm/java-8-oracle /usr/lib/jvm/oracle-jdk-8 && \
    rm -rf /var/cache/oracle-jdk8-installer/

FROM "065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:profile" AS PROFILERSETUP
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV JAVA_HOME=/usr/lib/jvm/java-8-oracle
COPY --from=0 /usr/lib/jvm/java-8-oracle /usr/lib/jvm/java-8-oracle

RUN chmod +x /tmp/benchmarks-build/install-profilers.sh && \
    /tmp/benchmarks-build/install-profilers.sh /usr/lib

