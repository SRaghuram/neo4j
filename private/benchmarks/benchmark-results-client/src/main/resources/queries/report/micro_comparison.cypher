// -------- Retrieve Latest Result For Previous Version (Baseline) --------
MATCH (tr:TestRun)-[:WITH_TOOL]->(:BenchmarkToolVersion)-[:VERSION_OF]->(:BenchmarkTool {name:'micro'}),
      (tr)-[:WITH_PROJECT]->(p:Project)
WHERE p.name='neo4j' AND 
      p.owner='neo4j' AND
      p.version=$old_version

MATCH (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WITH bg, b, m
ORDER BY tr.date DESC
WITH bg, b, head(collect(m)) AS m
ORDER BY bg.name, b.name
WITH collect({bg:bg,b:b,m:m}) AS results_old

// -------- Retrieve Latest Result For New Version --------
UNWIND range(0,size(results_old)-1) AS i
MATCH (p:Project)<-[:WITH_PROJECT]-(tr:TestRun),
      (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b:Benchmark)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE b.name=results_old[i].b.name AND
      bg.name=results_old[i].bg.name AND 
      p.name='neo4j' AND 
      p.owner='neo4j' AND
      p.version=$new_version
WITH bg, b, m, results_old[i].m AS m_old
ORDER BY tr.date DESC
WITH bg, b, head(collect({m_old:m_old,m_new:m})) AS m

RETURN bg.name AS group,
       split(b.name,'_(mode,')[0] AS bench,
       b.mode AS mode,
       m.m_old.mean AS old,
       m.m_old.unit AS old_unit,
       m.m_new.mean AS new,
       m.m_new.unit AS new_unit
