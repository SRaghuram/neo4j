MATCH  (:TestRun {id:{test_run_id}})-[:HAS_METRICS]->(m:Metrics)-[:METRICS_FOR]->(:Benchmark {name:{benchmark_name}})<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{benchmark_group_name}})
CREATE (m)-[:WITH_ANNOTATION]->(a:Annotation {annotation})
RETURN a