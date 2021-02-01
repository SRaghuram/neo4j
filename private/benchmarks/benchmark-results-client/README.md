##Java client for configuring, reading, writing benchmarks results store

Schema edit/generation
----------------------

Using http://www.apcjones.com/arrows/# 
* To edit: `Export Markup` copy-paste `schema.markup`
* To create relation: click on halo of existing node
* To delete: double click on node and click `delete` button

After editing commit markup and svg to the sources.

Build
-----

        mvn clean install
        
Prepare Schema
--------------
Assuming there is a Neo4j server running locally.
To create constraints and indexes on a store, for use as benchmark results storage database, do:

        java -jar target/benchmark-results-client.jar index --results-store-uri bolt://localhost

Refactoring Commands
--------------------

Show help:

```bash
java -jar target/benchmark-results-client.jar help refactor verify
java -jar target/benchmark-results-client.jar help refactor move-benchmark
```

Example usage:

```bash
java -jar target/benchmark-results-client.jar refactor move-benchmark \
                --benchmark-tool-name "micro" \
                --old-benchmark-group-name "Core API" \
                --new-benchmark-group-name "Catchup" \
                --benchmark-name "TxPullBare.pullTransactions" \
                --results-store-user "neo4j" \
                --results-store-pass "12345" \
                --results-store-uri "bolt://localhost:7687"
```
