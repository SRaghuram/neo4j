// --- Retrieve results for base version ---
MATCH (benchmark:Benchmark)<-[:HAS_BENCHMARK]-(group:BenchmarkGroup),
      (tr:TestRun)-[:HAS_METRICS]->(m:Metrics)-[:METRICS_FOR]->(benchmark),
      (tr)-[:WITH_PROJECT]->(project:Project)
WHERE group.name <> 'Index' AND
      (NOT group.name CONTAINS 'LdbcSnbInteractive' OR benchmark.name CONTAINS 'Summary') AND
      tr.triggered_by = 'neo4j' AND
      benchmark.active = true AND
      project.version = $oldVersion AND
      project.name = 'neo4j' AND
      project.owner = 'neo4j'
WITH group, benchmark, tr, m
ORDER BY tr.date DESC
WITH group, benchmark, head(collect({m:m,tr:tr})) AS latest
ORDER BY group.name, benchmark.name
WITH collect({group:group,
              benchmark:benchmark,
              mean:latest.m.mean,
              unit:latest.m.unit,
              date:latest.tr.date}) AS baseMetrics

// --- Retrieve results for new version ---
UNWIND baseMetrics AS baseMetric
WITH baseMetric.group AS group, baseMetric.benchmark AS benchmark, baseMetrics
OPTIONAL MATCH (project:Project)<-[:WITH_PROJECT]-(tr:TestRun)-[:HAS_METRICS]->(m:Metrics)-[:METRICS_FOR]->(benchmark)
WHERE tr.triggered_by = 'neo4j' AND
      project.version = $newVersion AND
      project.name = 'neo4j' AND
      project.owner = 'neo4j'
WITH group, benchmark, tr, m, baseMetrics
ORDER BY tr.date DESC
WITH group, benchmark, head(collect({m:m,tr:tr})) AS latest, baseMetrics
ORDER BY group.name, benchmark.name
WITH collect({group:group,
              benchmark:benchmark,
              mean:latest.m.mean,
              unit:latest.m.unit,
              date:latest.tr.date}) AS compareMetrics,
     baseMetrics

// --- Retrieve results for older version ---
UNWIND baseMetrics AS baseMetric
WITH baseMetric.group AS group,
     baseMetric.benchmark AS benchmark,
     baseMetrics,
     compareMetrics
OPTIONAL MATCH (project:Project)<-[:WITH_PROJECT]-(tr:TestRun)-[:HAS_METRICS]->(m:Metrics)-[:METRICS_FOR]->(benchmark)
WHERE project.version = $olderVersion AND
      project.name = 'neo4j' AND
      project.owner = 'neo4j'
WITH group, benchmark, tr, m, baseMetrics, compareMetrics
ORDER BY tr.date DESC
WITH group, benchmark, head(collect({m:m,tr:tr})) AS latest, baseMetrics, compareMetrics
ORDER BY group.name, benchmark.name
WITH collect({group:group,
              benchmark:benchmark,
              mean:latest.m.mean,
              unit:latest.m.unit,
              date:latest.tr.date}) AS olderCompareMetrics,
     baseMetrics,
     compareMetrics

UNWIND range(0,size(baseMetrics)-1) AS i
WITH baseMetrics[i].group.name AS Group,
     baseMetrics[i].benchmark.name AS Benchmark,
     CASE compareMetrics[i].mean IS NULL
       WHEN true THEN null
       ELSE
         CASE baseMetrics[i].benchmark.mode
           WHEN 'LATENCY' THEN toFloat(toInteger(toFloat(baseMetrics[i].mean/compareMetrics[i].mean)*100))/100
           WHEN 'THROUGHPUT' THEN toFloat(toInteger(toFloat(compareMetrics[i].mean/baseMetrics[i].mean)*100))/100
         END
     END AS Regression,
     CASE olderCompareMetrics[i].mean IS NULL
       WHEN true THEN null
       ELSE
         CASE baseMetrics[i].benchmark.mode
           WHEN 'LATENCY' THEN toFloat(toInteger(toFloat(olderCompareMetrics[i].mean/compareMetrics[i].mean)*100))/100
           WHEN 'THROUGHPUT' THEN toFloat(toInteger(toFloat(compareMetrics[i].mean/olderCompareMetrics[i].mean)*100))/100
         END
     END AS OldRegression
WHERE Regression > $limit
RETURN Group, Benchmark, Regression, OldRegression
ORDER BY Regression DESC, OldRegression DESC, Benchmark DESC