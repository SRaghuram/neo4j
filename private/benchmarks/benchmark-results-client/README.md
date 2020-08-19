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
