MATCH (tr:TestRun)-[:WITH_TOOL]->(btv:BenchmarkToolVersion),
      (btv)-[:VERSION_OF]->(:BenchmarkTool {name:'micro'}),
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
WITH collect({bg:bg,b:b,m:m}) AS results_old
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
WITH bg.name AS group,
     b,
     CASE
       WHEN b.mode IN ['LATENCY','SINGLE_SHOT'] THEN 'LATENCY'
       ELSE 'THROUGHPUT'
       END AS mode,
     m.m_old.mean AS result_old,
     m.m_new.mean AS result_new

// assume time unit is always the same
WITH group,
     b,
     CASE mode
       WHEN 'LATENCY'THEN
         CASE
           WHEN result_old > result_new THEN result_old/result_new
           ELSE                            result_new/result_old * -1
         END
       ELSE // 'THROUGHPUT'
         CASE
           WHEN result_old < result_new THEN result_new/result_old
           ELSE                            result_old/result_new * -1
         END
     END AS x

WITH group,
     b.simple_name AS bench,
     count(x) AS tests,
     min(x) AS min,
     max(x) AS max

WITH group,
     bench,
     tests,
     CASE
       WHEN min <= -1.5 THEN min
       ELSE 0
       END AS min,
     CASE
       WHEN max >= 1.5 THEN max
       ELSE 0
       END AS max

WITH group,
     bench,
     tests,
     CASE
       WHEN min >= 0 AND max > 0 THEN 4 // better
       WHEN min = 0 AND max = 0 THEN 3  // no change
       WHEN min < 0 AND max > 0 THEN 2  // mixed
       WHEN min < 0 AND max <= 0 THEN 1 // worse
     END AS change
ORDER BY change, tests DESC, group, bench

RETURN group AS Group,
       bench AS Bench,
       tests AS Tests,
       CASE change
         WHEN 4 THEN 'better'
         WHEN 3 THEN 'unchanged'
         WHEN 2 THEN 'mixed'
         WHEN 1 THEN 'worse'
       END AS change