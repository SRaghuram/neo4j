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

// for each benchmark, retrieve first test run following the given packaging build
WITH bt, bg, b, tr
ORDER BY tr.date ASC
WITH bt, bg, b, head(collect(tr)) AS tr

// multiple benchmarks may have been last executed in the same test run, and DISTINCT is cheaper than MERGE
WITH DISTINCT tr

MERGE (tr)-[:WITH_ANNOTATION]->(a:Annotation {comment:$comment})
  ON CREATE SET a+={date:timestamp(),event_id:randomUUID(),author:$author}
