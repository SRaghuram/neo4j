MATCH (tr:TestRun)-[:WITH_TOOL]->(:BenchmarkToolVersion)-[:VERSION_OF]->(:BenchmarkTool {name:'macro'}),
      (tr)-[:WITH_PROJECT]->(p:Project)
WHERE p.name='neo4j' AND
      p.owner='neo4j' AND
      p.branch=$old_branch
MATCH (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (m)-[:HAS_PLAN]->(plan:Plan),
      (b)-[:HAS_PARAMS]->(bp:BenchmarkParams),
      (b)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE bp.execution_mode=$execution_mode AND
      bp.runtime='DEFAULT' AND
      bp.planner='DEFAULT' AND
      bp.deployment=$deployment
WITH bg, b, m, plan
ORDER BY tr.date DESC
WITH bg, b, head(collect({m:m,p:plan})) AS result
WITH collect({bg:bg,b:b,m:result.m,p:result.p}) AS results_old
UNWIND range(0,size(results_old)-1) AS i
MATCH (p:Project)<-[:WITH_PROJECT]-(tr:TestRun),
      (tr)-[:HAS_METRICS]->(m:Metrics),
      (m)-[:METRICS_FOR]->(b:Benchmark),
      (m)-[:HAS_PLAN]->(plan:Plan),
      (b:Benchmark)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)
WHERE b.name=results_old[i].b.name AND
      bg.name=results_old[i].bg.name AND
      p.name='neo4j' AND
      p.owner='neo4j' AND
      p.branch=$new_branch
WITH bg, b, m, plan, results_old[i].m AS m_old, results_old[i].p AS p_old
ORDER BY tr.date DESC
WITH bg, b, head(collect({m_old:m_old,p_old:p_old,m_new:m,p_new:plan})) AS result
WITH bg.name AS group,
     b AS b,
     result.m_old.mean AS result_old,
     result.m_old.unit AS result_old_unit,
     result.p_old AS old_plan,
     result.m_new.mean AS result_new,
     result.m_new.unit AS result_new_unit,
     result.p_new AS new_plan

// assume time unit is always the same
WITH group,
     b,
     result_old AS old,
     result_new AS new,
     result_new_unit AS unit,
     old_plan,
     new_plan,
     CASE
       WHEN result_old > result_new THEN result_old/result_new      // better
       ELSE                             result_new/result_old * -1  // worse
       END AS x

// 'x' is a multiple in the range (1..big)
WHERE abs(x) >= $difference_threshold

MATCH (new_plan)-[:HAS_PLAN_TREE]->(:PlanTree)-[:HAS_OPERATORS]->(:Operator)-[:HAS_CHILD*]->(operator:Operator)

RETURN group as Group,
       b.simple_name AS Bench,
       CASE
         WHEN NOT old_plan.used_runtime = new_plan.used_runtime THEN old_plan.used_runtime + ' --> ' + new_plan.used_runtime
         ELSE ''
         END AS Runtime,
       round(100 * old) / 100 AS `4.0 latency`,
       collect(DISTINCT operator.operator_type) AS Operators,
       toFloat(toInteger(x * 10))/10 AS Improvement

ORDER BY Improvement DESC, Group, Bench