MATCH (n:Project)
WHERE n.version STARTS WITH {neo4j_series} AND n.owner={owner} AND n.name = 'neo4j'
WITH collect(n) AS ns
MATCH (bp:BenchmarkParams)<-[:HAS_PARAMS]-(b:Benchmark)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup),
      (b)<-[:METRICS_FOR]-(:Metrics)<-[:HAS_METRICS]-(:TestRun)-[:WITH_PROJECT]->(n)
WHERE bg.name={group} AND n IN ns AND b.active=true
RETURN DISTINCT b, bp
