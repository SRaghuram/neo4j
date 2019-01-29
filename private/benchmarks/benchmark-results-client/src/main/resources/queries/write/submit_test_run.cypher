MERGE (environment:Environment {operating_system:{operating_system}, server:{server}})
MERGE (benchmark_tool:BenchmarkTool {name: {tool_name}})
ON CREATE SET
    benchmark_tool.repository_name={tool_repository_name}
MERGE (benchmark_tool_version:BenchmarkToolVersion {commit: {tool_commit},
                                                    owner: {tool_owner},
                                                    branch: {tool_branch}})-[:VERSION_OF]->(benchmark_tool)

MERGE (java:Java {jvm: {jvm}, version:{jvm_version}, args:{jvm_args}})
CREATE (test_run:TestRun {test_run})
FOREACH (project IN {projects} |
    MERGE (p:Project {commit: project.commit,
                     edition: project.edition,
                     owner: project.owner,
                     name: project.name,
                     version: project.version,
                     branch: project.branch})
    CREATE (p)<-[:WITH_PROJECT]-(test_run)
)
CREATE
    (benchmark_config:BenchmarkConfig {benchmark_config}),
    (base_neo4j_config:Neo4jConfig {base_neo4j_config}),
    (test_run)-[:HAS_BENCHMARK_CONFIG]->(benchmark_config),
    (test_run)-[:HAS_CONFIG]->(base_neo4j_config),
    (test_run)-[:WITH_TOOL]->(benchmark_tool_version),
    (test_run)-[:WITH_JAVA]->(java),
    (test_run)-[:IN_ENVIRONMENT]->(environment)
WITH test_run, benchmark_tool
UNWIND {metrics_tuples} AS metrics_tuple
WITH test_run,
     benchmark_tool,
     metrics_tuple[0] AS benchmarkGroupName,
     metrics_tuple[1].name AS benchmarkName,
     metrics_tuple[1].description AS benchmarkDescription,
     metrics_tuple[1].cypher AS cypherQuery,
     metrics_tuple[1] AS benchmarkProperties,
     metrics_tuple[2] AS benchmarkParams,
     metrics_tuple[3] AS metricsValues,
     metrics_tuple[4] AS neo4jBenchmarkConfig,
     // hacky hack for dealing with conditional updates
     CASE length(metrics_tuple) WHEN 6 THEN [metrics_tuple[5]] ELSE [] END AS profiles_maps
MERGE (benchmark_group:BenchmarkGroup {name:benchmarkGroupName})<-[:IMPLEMENTS]-(benchmark_tool)
MERGE (benchmark_group)-[:HAS_BENCHMARK]->(benchmark:Benchmark {name:benchmarkName})-[:HAS_PARAMS]->(params:BenchmarkParams)
// Is new benchmark
ON CREATE SET
    benchmark=benchmarkProperties,
    params=benchmarkParams
// Description overwritten on purpose. It may get updated/corrected over time, database should reflect these changes
ON MATCH SET
   benchmark.description=benchmarkDescription,
   benchmark.cypher_query=cypherQuery
CREATE (test_run)-[:HAS_METRICS]->(metrics:Metrics)-[:METRICS_FOR]->(benchmark)
SET
    metrics = metricsValues
CREATE (metrics)-[:HAS_CONFIG]->(neo4jConfig:Neo4jConfig)
SET
    neo4jConfig = neo4jBenchmarkConfig
WITH
    test_run,
    benchmark,
    metrics,
    params,
    profiles_maps
FOREACH ( profiles_map IN profiles_maps |
    CREATE (metrics)-[:HAS_PROFILES]->(profiles:Profiles)
    SET profiles = profiles_map)
RETURN test_run, collect([benchmark, metrics, params]) AS benchmark_metrics
