// Get mean latency for 100 latest benchmarks of some benchmark group
// Only consider benchmarks that were run against some Neo4j version
// Order results by date
MATCH (n:Project {version:{neo4j_version}, name:'neo4j'})<-[:WITH_PROJECT]-(tr:TestRun)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark)<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
RETURN tr AS test_run, b AS benchmark, m AS metrics, n AS neo4j
ORDER BY tr.date
LIMIT 100