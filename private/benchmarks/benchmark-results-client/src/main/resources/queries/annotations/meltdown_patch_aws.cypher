MATCH (e:Environment)<-[:IN_ENVIRONMENT]-(tr:TestRun),
      (tr)-[:WITH_TOOL]->(:BenchmarkTool {name:'ronja-benchmarks'}),
      (tr)-[:WITH_PROJECT]->(n:Project {name: 'neo4j'}),
      (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE e.operating_system CONTAINS 'Linux, 4.4.0-1047-aws' AND
      n.owner='neo4j'
WITH n, tr, m, b, bg, e
ORDER BY tr.date ASC
WITH n.branch AS branch,
     b,
     bg,
     head(collect(tr)) AS tr
// Filter out TestRun nodes that already have the Annotation
OPTIONAL MATCH (b)<-[:METRICS_FOR]-(:Metrics)<-[:HAS_METRICS]-(tempTr:TestRun)-[:WITH_ANNOTATION]->(a:Annotation),
               (tempTr)-[:WITH_PROJECT]->(:Project {branch:branch, name:'neo4j'})
WHERE a.comment='Meltdown Patch' AND
      a.author='alex.averbuch@neo4j.com'
WITH branch, b, bg, tr
WHERE tempTr IS NULL
CALL bench.createTestRunAnnotation(tr.id,'Meltdown Patch','alex.averbuch@neo4j.com')
YIELD annotation
RETURN annotation
