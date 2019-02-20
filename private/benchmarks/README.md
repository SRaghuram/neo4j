## The Benchmarking repo for Neo4j

This repository contains all the projects needed to create and run benchmarks.
It also contains the procedures used by [Alacrity (benchmark results UI)](http://alacrity.neohq.net).

Build and run
-----

This repository contains multiple modules, to build them all just do the regular.

        mvn clean package -DskipTests

Alternatively, you may want to build just a subset of them.
See below for commands you'll need to build and package your benchmarks.

#####Micro

        mvn clean package -pl benchmark-results-client -pl micro -DskipTests

There are instructions on how to run in [micro/README](micro/README.md)

#####LDBC

        mvn clean package -pl benchmark-results-client -pl ldbc/neo4j-connectors
        
There are instructions on how to run in [ldbc/README](ldbc/README.md)

#####Macro

        mvn clean package -pl benchmark-results-client -pl macro -DskipTests
        
There are instructions on how to run in [macro/README](macro/README.md)

#####Procedures 

        mvn clean package -pl benchmark-results-client -pl benchmark-procedures -DskipTests      

#####Result Client

        mvn clean package -pl benchmark-results-client -DskipTests
        
Installing Profilers
--------------
To be able to run collect profiler recordings when running locally, profilers must be installed.

To do so, please refer to installation instructions in [benchmark-agent-automation/README](benchmark-agent-automation/README.md)

#####Async Profiler
The automated installation script does not yet support installation of async-profiler.

To do so, please follow the following steps:
```
echo "kernel.perf_event_paranoid=1" >> /etc/sysctl.d/benchmark-profiling.conf
echo "kernel.kptr_restrict=0" >> /etc/sysctl.d/benchmark-profiling.conf
sysctl -p /etc/sysctl.conf
(cd /tmp && curl --fail --silent --show-error --retry 5 --remote-name --location https://github.com/jvm-profiling-tools/async-profiler/archive/v1.4.tar.gz)
mkdir --parents /usr/lib/async-profiler
tar -C /usr/lib/async-profiler --strip-components=1 -xzf /tmp/v1.4.tar.gz
(cd /usr/lib/async-profiler && make)
```
Then set `ASYNC_PROFILER_DIR` to the directory of async-profiler
