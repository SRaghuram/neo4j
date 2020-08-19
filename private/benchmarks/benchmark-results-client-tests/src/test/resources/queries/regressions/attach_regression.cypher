MATCH (:TestRun {id:$test_run_id})-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark {name:$benchmark_name}),
      (b)<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:$benchmark_group_name})
CREATE (m)-[:IS_REGRESSION]->(a:Regression $regression)
