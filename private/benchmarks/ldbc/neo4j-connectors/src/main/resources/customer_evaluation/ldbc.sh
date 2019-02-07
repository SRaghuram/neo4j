#!/usr/bin/env bash
set -e
set -u

java -Xmx4g -jar ldbc.jar run \
    --ldbc-config ldbc_snb_interactive_SF-0001-read-cypher.properties \
    --reads parameters \
    --results ldbc_results \
    --read-threads 8 \
    --warmup-count 10000 \
    --run-count 10000 \
    --neo4j-config neo4j_sf001.conf \
    --db graph.db \
    --neo4j-api EMBEDDED_CYPHER
