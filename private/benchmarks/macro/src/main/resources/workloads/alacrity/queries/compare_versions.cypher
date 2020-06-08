UNWIND $versionPrefixes AS versionPrefix
MATCH (project:Project)
WHERE project.version STARTS WITH versionPrefix AND
      project.owner='neo4j'
WITH collect(DISTINCT project) AS projects

MATCH (bt:BenchmarkTool)-[:IMPLEMENTS]->(group:BenchmarkGroup)-[:HAS_BENCHMARK]->(benchmark:Benchmark)-[:HAS_PARAMS]->(benchmarkParams:BenchmarkParams),
      (project:Project)<-[:WITH_PROJECT]-(testRun:TestRun)-[:HAS_METRICS]->(metrics:Metrics)-[:METRICS_FOR]->(benchmark)
WHERE project IN projects AND
      benchmark.active = true AND
      testRun.triggered_by <> 'new_infra' AND
      bt.name = $benchmarkToolName AND
      group.name = $benchmarkGroupName
WITH testRun, metrics, project, benchmark, benchmarkParams, projects
ORDER BY testRun.date DESC
WITH project.version AS projectVersion,
     project.owner AS projectOwner,
     project.branch AS projectBranch,
     testRun.triggered_by AS triggered_by,
     benchmark,
     head(collect([metrics,project,benchmarkParams,testRun])) AS latestResult
WITH projectOwner,
     benchmark,
     latestResult[0] AS metrics,
     latestResult[1] AS project,
     latestResult[2] AS benchmarkParams,
     latestResult[3].triggered_by AS triggeredBy
RETURN benchmark.name AS benchmarkName,
       benchmark.simple_name AS benchmarkSimpleName,
       benchmark.mode AS benchmarkMode,
       benchmarkParams {.*} AS benchmarkParams,
       collect([project.owner,
                project.branch,
                project.version,
                metrics.unit,
                metrics.mean,
                triggeredBy]) AS versionsData