// Get 100 latency result metrics for some benchmark and some benchmark group
// Only consider benchmarks that were run against some Neo4j version
// Order results by date
MATCH (b:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
WITH b
LIMIT 1
MATCH (m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(n:Project)
WHERE (m)-[:METRICS_FOR]->(b)
RETURN tr AS test_run, n AS neo4j, m AS metrics
ORDER BY test_run.date
LIMIT 100
