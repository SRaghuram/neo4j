MATCH (j:Java)<-[:WITH_JAVA]-(tr:TestRun),
      (tr)-[:WITH_PROJECT]->(n:Project),
      (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
    WHERE j.version CONTAINS '11.0.2,' AND
    n.owner = 'neo4j' AND
    n.name = 'neo4j' AND
    tr.triggered_by = 'neo4j' AND
    n.version = '4.0.0'
WITH n, tr, m, b, bg, j
    ORDER BY tr.date ASC
WITH n.branch AS branch,
     b,
     bg,
     head(collect(tr)) AS tr
// Filter out TestRun nodes that already have the Annotation
OPTIONAL MATCH (b)<-[:METRICS_FOR]-(:Metrics)<-[:HAS_METRICS]-(tempTr:TestRun)-[:WITH_ANNOTATION]->(a:Annotation),
               (tempTr)-[:WITH_PROJECT]->(:Project {branch: branch, name: 'neo4j', version: '4.0.0'})
    WHERE a.comment = 'Java 11' AND
    a.author = 'simon.svensson@neo4j.com'
WITH DISTINCT tr, tempTr
    WHERE tempTr IS NULL
CREATE (tr)-[:WITH_ANNOTATION]->(a:Annotation)
SET a.comment = 'Java 11'
SET a.author = 'simon.svensson@neo4j.com'
RETURN tr, a