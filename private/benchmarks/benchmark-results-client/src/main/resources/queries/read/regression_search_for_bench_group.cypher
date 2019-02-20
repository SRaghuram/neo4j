// Given some benchmark, some benchmark group, and some Neo4j version, retrieve four results:
// (1) FASTEST: result with lowest mean
// (2) LATEST: most recent result
// (3) RECENT FASTER: latest result with lower mean (within tolerance) than the most recent result -- if one exists
// (4) REGRESSION: result immediately following RECENT FASTER -- if one exists
MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
RETURN 'FASTEST' AS ref, tr, m
ORDER BY m.mean ASC
LIMIT 1

UNION ALL

MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
RETURN  'LATEST' AS ref, tr, m
ORDER BY tr.date DESC
LIMIT 1

UNION ALL

MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
WITH tr, m.mean AS latest_mean
ORDER BY tr.date DESC
LIMIT 1
WITH latest_mean
MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
OPTIONAL MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
WHERE m.mean < latest_mean - {tolerance}
RETURN 'RECENT FASTER' AS ref, tr, m
ORDER BY tr.date DESC
LIMIT 1

UNION ALL

MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
WITH tr, m.mean AS latest_mean
ORDER BY tr.date DESC
LIMIT 1
WITH latest_mean
MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
OPTIONAL MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
WHERE m.mean < latest_mean - {tolerance}
WITH tr AS faster_tr
ORDER BY tr.date DESC
LIMIT 1
MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
OPTIONAL MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(:Project {version:{neo4j_version},name:'neo4j'})
WHERE tr.date > faster_tr.date
RETURN 'REGRESSION' AS ref, tr, m
ORDER BY tr.date ASC
LIMIT 1
