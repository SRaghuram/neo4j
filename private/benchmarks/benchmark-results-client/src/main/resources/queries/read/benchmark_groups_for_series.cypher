MATCH (n:Project )
    WHERE n.version STARTS WITH {neo4j_series} AND n.owner = 'neo4j' AND name = 'neo4j'
WITH collect(n) AS ns
MATCH (b:Benchmark)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup),
      (b)<-[:METRICS_FOR]-(:Metrics)<-[:HAS_METRICS]-(:TestRun)-[:WITH_PROJECT]->(n)
WHERE n IN ns AND b.active=true
RETURN DISTINCT bg
