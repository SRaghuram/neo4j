MATCH (tr:TestRun)-[:WITH_PROJECT]->(p:Project),
      (tr)-[:WITH_TOOL]->(:BenchmarkToolVersion)-[:VERSION_OF]->(bt:BenchmarkTool),
      (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE tr.parent_build >= $packaging_build_id AND
      p.name='neo4j' AND
      p.owner='neo-technology' AND
      p.branch=$neo4j_branch AND
      bt.name IN $benchmark_tools

// for each benchmark, retrieve result following the given packaging build
WITH bg, b, m
ORDER BY tr.date ASC
WITH bg, b, head(collect(m)) AS m

MERGE (m)-[:WITH_ANNOTATION]->(a:Annotation {comment:$comment})
  ON CREATE SET a+={date:timestamp(),event_id:randomUUID(),author:$author}
