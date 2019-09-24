// -------- Retrieve Latest Result For Previous Version (Baseline) --------
MATCH (tr:TestRun)-[:WITH_TOOL]->(:BenchmarkToolVersion)-[:VERSION_OF]->(:BenchmarkTool {name:'macro'}),
      (tr)-[:WITH_PROJECT]->(p:Project)
WHERE p.name='neo4j' AND
      p.owner='neo4j' AND
      p.version=$old_version

MATCH (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b)-[:HAS_PARAMS]->(bp:BenchmarkParams),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE bp.execution_mode='EXECUTE' AND
      bp.runtime='DEFAULT' AND
      bp.planner='DEFAULT' AND
      bp.deployment='EMBEDDED'
WITH bg, b, m
ORDER BY tr.date DESC
WITH bg, b, head(collect(m)) AS m_old

// -------- Retrieve Latest Result For New Version --------
MATCH (p:Project)<-[:WITH_PROJECT]-(tr:TestRun),
      (tr)-[:HAS_METRICS]->(m_new:Metrics),
      (m_new)-[:METRICS_FOR]->(b)<-[:HAS_BENCHMARK]-(bg)
WHERE p.name='neo4j' AND
      p.owner='neo4j' AND
      p.version=$new_version
WITH bg, b, m_new, m_old
ORDER BY tr.date DESC
WITH bg, b, head(collect({m_old:m_old,m_new:m_new})) AS m
RETURN bg.name AS group,
       b.simple_name AS bench,
       b.description AS description,
       b.mode AS mode,
       m.m_old.mean AS old,
       m.m_old.unit AS old_unit,
       m.m_new.mean AS new,
       m.m_new.unit AS new_unit
