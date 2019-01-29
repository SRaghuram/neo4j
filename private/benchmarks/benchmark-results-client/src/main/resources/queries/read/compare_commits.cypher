// Given two commits, some benchmark group, some server, and some operating system,
// Retrieve full result metrics for every benchmark, for both commits
MATCH (n:Project {name:'neo4j'})<-[:WITH_PROJECT]-(tr:TestRun)-[:HAS_METRICS]->(m:Metrics)-[:METRICS_FOR]->(b:Benchmark),
      (b)-[:HAS_PARAMS]->(p:BenchmarkParams)
WHERE (b)<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name:{group_name}}) AND n.commit IN {commits}
WITH n.commit AS neo4j_commit,
     n.version AS neo4j_version,
     n.edition AS neo4j_edition,
     n.branch AS neo4j_branch,
     n.owner AS neo4j_owner,
     tr.id AS run,
     tr.date AS run_date,
     tr.duration AS run_duration,
     tr.build AS teamcity_build,
     b AS benchmark,
     p AS params,
     m AS metrics
ORDER BY benchmark.name
RETURN neo4j_commit,
       neo4j_version,
       neo4j_edition,
       neo4j_branch,
       neo4j_owner,
       teamcity_build,
       run,
       run_date,
       run_duration,
       collect([benchmark,metrics,params]) AS benchmarks
