// Get latest result for some benchmark and some benchmark group
// Only consider benchmarks that were run against some Neo4j version on some server
// Compare against,
// Latest result for some benchmark and some benchmark group
// Only consider benchmarks that were run against some Neo4j commit on some server
MATCH (n:Project {version:{neo4j_version}})<-[:WITH_PROJECT]-(tr:TestRun)-[:HAS_METRICS]->(m:Metrics)
WHERE (tr)-[:IN_ENVIRONMENT]->(:Environment {server:{server}}) AND
      (m)-[:METRICS_FOR]->(:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
RETURN tr AS test_run, n AS neo4j, m AS metrics
ORDER BY test_run.date
LIMIT 1
UNION ALL
MATCH (n:Project {commit:{neo4j_commit}})<-[:WITH_PROJECT]-(tr:TestRun)-[:HAS_METRICS]->(m:Metrics)
WHERE (tr)-[:IN_ENVIRONMENT]->(:Environment {server:{server}}) AND
      (m)-[:METRICS_FOR]->(:Benchmark {name:{bench_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}})
RETURN tr AS test_run, n AS neo4j, m AS metrics
ORDER BY test_run.date
LIMIT 1
