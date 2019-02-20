##Java client for configuring, reading, writing benchmarks results store

Build
-----

        mvn clean install
        
Prepare Schema
--------------
Assuming there is a Neo4j server running locally.
To create constraints and indexes on a store, for use as benchmark results storage database, do:

        java -jar target/benchmark-results-client.jar index --results_store_uri bolt://localhost
