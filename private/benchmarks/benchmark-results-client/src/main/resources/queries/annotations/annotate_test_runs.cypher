MATCH (tr:TestRun)-[:WITH_PROJECT]->(p:Project),
      (tr)-[:WITH_TOOL]->(:BenchmarkToolVersion)-[:VERSION_OF]->(bt:BenchmarkTool)
WHERE tr.parent_build >= $packaging_build_id AND
      p.name='neo4j' AND
      p.owner='neo4j' AND
      p.branch=$neo4j_branch AND
      bt.name IN $benchmark_tools

WITH bt, tr
ORDER BY tr.date ASC
WITH bt, head(collect(tr)) AS tr
MERGE (tr)-[:WITH_ANNOTATION]->(a:Annotation {comment:$comment})
  ON CREATE SET a+={date:timestamp(),event_id:randomUUID(),author:$author}
