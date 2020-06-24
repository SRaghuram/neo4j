MATCH (tr:TestRun)-[:WITH_TOOL]->(btv:BenchmarkToolVersion),
      (btv)-[:VERSION_OF]->(:BenchmarkTool {name:'macro'}),
      (tr)-[:WITH_PROJECT]->(p:Project)
WHERE p.name='neo4j' AND
      p.owner='neo4j' AND
      p.version=$version

MATCH (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (b)-[:HAS_PARAMS]->(bp:BenchmarkParams),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE bp.execution_mode='EXECUTE' AND
      bp.runtime='DEFAULT' AND
      bp.planner='DEFAULT' AND
      bp.deployment=$deployment
WITH bg, b, m
ORDER BY tr.date DESC
WITH bg, b, head(collect(m)) AS m

WHERE (m)-[:HAS_PLAN]->(:Plan)-[:HAS_PLAN_TREE]->(:PlanTree)-[:HAS_OPERATORS]->(:Operator/*ProduceResults*/)-[:HAS_CHILD*0..]->(:Operator {operator_type:$operator_name})

RETURN DISTINCT bg.name AS group, count(DISTINCT b.simple_name) AS benchmarks
ORDER BY benchmarks DESC, group