MATCH (bg:BenchmarkGroup {name:{group_name}})-[:HAS_BENCHMARK]->(b:Benchmark {name:{benchmark_name}})
WITH bg, b
MATCH (n:Project {name:'neo4j'})
WHERE n.version STARTS WITH {neo4j_series} AND n.owner={owner}
WITH collect(n) AS ns, b, bg
MATCH (b)<-[:METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(n:Project {name:'neo4j'})
WHERE n IN ns
RETURN m.mean AS mean, tr.date AS date, m.unit AS unit, id(m) AS metricsNodeId, n AS neo4j
ORDER BY date ASC
