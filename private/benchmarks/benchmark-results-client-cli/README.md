## Java client for configuring, reading, writing benchmarks results store

### Build

```shell
mvn clean install
```

### Prepare Schema

Assuming there is a Neo4j server running locally. To create constraints and indexes on a store, for use as benchmark results storage database, do:

```shell
java -jar target/benchmark-results-client-cli.jar index --results-store-uri bolt://localhost
```

### Refactoring Commands

Show help:

```shell
java -jar target/benchmark-results-client-cli.jar help refactor verify
java -jar target/benchmark-results-client-cli.jar help refactor move-benchmark
```

Example usage:

```shell
java -jar target/benchmark-results-client-cli.jar refactor move-benchmark \
                --benchmark-tool-name "micro" \
                --old-benchmark-group-name "Core API" \
                --new-benchmark-group-name "Catchup" \
                --benchmark-name "TxPullBare.pullTransactions" \
                --results-store-user "neo4j" \
                --results-store-pass "12345" \
                --results-store-uri "bolt://localhost:7687"
```
