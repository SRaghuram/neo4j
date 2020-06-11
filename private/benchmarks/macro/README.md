Macro Benchmarks
--------------
Available Commands
--------------
Make sure that you have build with the flag `-PfullBenchmarks`

        $java -jar macro.jar help
        usage: bench <command> [<args>]

        The most commonly used bench commands are:
            help            Display help information
            run-single      runs one query in a new process for a single workload
            run-workload    runs all queries for a single workload
            upgrade-store   Upgrades a Neo4j store, including rebuilding of indexes.
        
        See 'bench help <command>' for more information on a specific command.
Running locally
--------------
The easiest way to run Macro locally is to use `ConvenientLocalExecutionIT`. 
You will also need to download the stores that are used in the benchmarks.
You can download these from S3

https://s3.console.aws.amazon.com/s3/buckets/benchmarking.neo4j.com/datasets/macro/?region=us-east-1&tab=overview 

Make sure to use the #.#-enterprise-datasets.

In `ConvenientLocalExecutionIT` there are configuration options for the workloads.

Running in batch infra
---------------------

If you want to run all benchmarks from your branch or specific build in batch infrastructure you can do so, with a little
bit of help from two scripts.

What you need is:

* neo4j-dev AWS account with batch and S3 service permissions
* AWS CLI installed on your machine
* bash

First you need to build your product and prepare workspace with all benchmarking artifacts:

    ./prepare-macro-workspace.sh --workspace-dir [workspace-dir]

Once you have workspace, next you need to schedule run of all benchmarks, with:

    ./schedule-all-macro-benchmarks.sh --workspace-dir [workspace-dir] --branch-owner [branch-owner] --neo4j-branch [neo4j-branch] --neo4j-version [neo4j-version]
    
It will take sometime to schedule all runs, once it is done, you can either exit or watch for progress. Even if you exit,
benchmarks will be still scheduled for run, which you can check in AWS Batch dashboard.

Adding new queries
--------------
Under `macro/src/resources/workloads/` there is one folder per workload that can be run.
Every workload has a file named `<workload-name>.json`. 
This file contains a JSON list of the queries that this workload can run.
To add a query you need to add it to this file and inside of the `queries/` folder of that workload.

About the workloads
--------------

#### accesscontrol
[A real-world use case from the Graph Databases book, inspired by Telenor.](https://github.com/iansrobinson/graph-databases-use-cases)

---
#### bubble_eye
A customer dataset. Private do not share.

---
#### cineasts
[The full dataset (12k movies, 50k actors) of the Spring Data Neo4j Cineasts.net tutorial.](https://docs.spring.io/spring-data/data-graph/snapshot-site/reference/html/#tutorial)

---
#### cineasts_csv
[The full dataset (12k movies, 50k actors) of the Spring Data Neo4j Cineasts.net tutorial.]( https://docs.spring.io/spring-data/data-graph/snapshot-site/reference/html/#tutorial)

This benchmark builds the database from scratch using `LOAD CSV`.

---
#### elections
[US Federal Elections & Campaigns 2012.](https://neo4j.com/blog/follow-the-data-fec-campaign-data-challenge/) 

---
#### generated_queries
Uses [the pokec dataset from the Stanford social network from Slovakia.](https://snap.stanford.edu/data/soc-pokec.html)

This benchmark generates a different query (mutates the Cypher string) on every execution.

---
#### generatedmusicdata_read
Read-only queries, executed against the synthetically-generated music dataset.

---
#### generatedmusicdata_write
Write queries, executed against the synthetically-generated music dataset.

---
#### grid
A square grid with nodes of label `:Person` and property `name="c(x,y)"`, relationships from cell to cell, left to right and top to bottom. 
Good for benchmarking `shortestPath()` and very long pattern expressions.

---
#### index_backed_order_by
Uses [the pokec dataset from the Stanford social network from Slovakia.](https://snap.stanford.edu/data/soc-pokec.html)

A workload dedicated to testing the index-back order by feature. 

---
#### ldbc_sf001
Read-only component of the [LDBC benchmark @ Scale Factor 1 (~2GB store)](https://sites.google.com/a/neotechnology.com/intranet/ldbc?pli=1)

---
#### ldbc_sf010
Read-only component of the [LDBC benchmark @ Scale Factor 10 (~20GB store)](https://sites.google.com/a/neotechnology.com/intranet/ldbc?pli=1)

---
#### levelstory
[A customer dataset (artificial data)](https://levelstory.com/)

---
#### logistics
[A real-world use case from the Graph Databases book, inspired by our logistics customers.](https://github.com/iansrobinson/graph-databases-use-cases)

---
#### musicbrainz
[A large music dataset, capturing information about artists, their recorded works, and the relationships between them.](https://neo4j.com/blog/musicbrainz-in-neo4j-part-1/)

---
#### nexlp
A customer dataset. Private do not share.

---
#### osmnodes
A dataset of all nodes in a March 2018 download of OSM data for North America and Europe. 
Nodes with any tags were selected, leading to about 75 million nodes. 
Each node has the following properties:

 * `osm_id`: The OSM internal id - as a unique constraint
 * `location`: The geographic location of a node using Neo4j `Point(WGS-84)` type
 * `created`: The date and time when this node was created, using Neo4j `DateTime` type
 * `name`: The value of the tag name if it exists
 * `place`: The value of one of several other tags searched for: place, amenity, restaurant, shop, building, capital, information, description, station, type, kiosk, office, location, food
Each of the properties is added to an index for faster searching and for benchmarking the indexes of the new Point and DateTime types.

---

#### pokec_read
[This is the pokec dataset from the Stanford social network from Slovakia. Read only queries.](https://snap.stanford.edu/data/soc-pokec.html)

---
#### pokec_write
[This is the pokec dataset from the Stanford social network from Slovakia. Write queries allowed.](https://snap.stanford.edu/data/soc-pokec.html)

---
#### qmul_read
Queen Mary University of London dataset exhibiting pathological performance read queries. Private do not share.

---
#### qmul_write
Queen Mary University of London dataset exhibiting pathological performance. Private do not share.

---
#### recommendations
[Rik's recommendation data.](http://blog.bruggen.com/2014/09/graphs-for-hr-analytics.html)

---
#### socialnetwork

[A real-world use case from the Graph Databases book. This encompasses bits from Adobe, as well as another client "X"; sub-domains/entities of interest include HR, recruitment, skills and projects.](https://github.com/iansrobinson/graph-databases-use-cases)

---
#### offshore_leaks

[A real-world use case with Panama papers and Paradise papers](https://offshoreleaks.icij.org/pages/database)
[Example queries](https://offshoreleaks-data.icij.org/offshoreleaks/neo4j/guide/examples.html) which were used to create the workload.
---
#### fraud-poc-credit
Based on a customer POC acquired from field team.
Basic stats of the 'fraud-poc' dataset:
 * Size: ~24 GB
 * Nodes: ~64,000,000
 * Relationships: ~192,000,000
The full POC contained two groups of queries named 'credit fraud' and 'data science'. 
The workload contains queries from the 'credit fraud' group only. 
---

#### alacrity

Based on our internal [benchmark reporting infrastructure, 'Alacrity'](http://benchmarking.neohq.net/).
The dataset is a snapshot of the results store from 2020-6-8:
 * Size: ~24 GB
 * Nodes: ~37,000,000
 * Relationships: ~53,000,000
Queries are taken from both the UI (read queries used to create reports) and the "result client" (various updates) which reports results.