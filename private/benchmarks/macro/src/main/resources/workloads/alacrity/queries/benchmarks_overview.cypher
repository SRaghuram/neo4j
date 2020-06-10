// --- Convert version prefixes to specific versions ---
UNWIND $neo4jVersionPrefixes AS neo4jVersionPrefix
MATCH (project:Project )<-[:WITH_PROJECT]-(tr:TestRun)
WHERE project.owner = 'neo4j' AND project.version STARTS WITH neo4jVersionPrefix
WITH collect(DISTINCT [project.owner, project.branch, project.version, tr.triggered_by]) AS neo4jVersions

// --- Retrieve benchmarks ---
MATCH (b:Benchmark)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)<-[:IMPLEMENTS]-(bt:BenchmarkTool)
WHERE bt.name = $benchmarkToolName AND
      bg.name = $benchmarkGroup AND
      b.name STARTS WITH $benchmarkNamePrefix AND
      b.active = true
WITH collect(b) AS benchmarks, neo4jVersions

// --- Retrieve test runs and their annotations ---
MATCH (project:Project)<-[:WITH_PROJECT]-(tr:TestRun)
WHERE [project.owner, project.branch, project.version, tr.triggered_by] IN neo4jVersions AND
      tr.triggered_by <> 'new_infra'
OPTIONAL MATCH (tr)-[:WITH_ANNOTATION]->(testRunAnnotation:Annotation)
WITH project, tr, benchmarks, collect(testRunAnnotation {.*}) AS testRunAnnotations

MATCH (bt:BenchmarkTool)<-[:VERSION_OF]-(btv:BenchmarkToolVersion)<-[WITH_TOOL]-(tr)-[:HAS_METRICS]->(m:Metrics)-[:METRICS_FOR]->(b:Benchmark),
      (java:Java)<-[:WITH_JAVA]-(tr)-[IN_ENVIRONMENT]->(e:Environment),
      (m:Metrics)-[HAS_CONFIG]->(neo4jConfig:Neo4jConfig)
WHERE b IN benchmarks
OPTIONAL MATCH (m)-[:HAS_PROFILES]->(profiles:Profiles)
OPTIONAL MATCH (m)-[:WITH_ANNOTATION]->(metricsAnnotation:Annotation)
OPTIONAL MATCH (m)-[:HAS_AUXILIARY_METRICS]->(auxiliaryMetrics:AuxiliaryMetrics)
OPTIONAL MATCH (m)-[:HAS_PLAN]->(plan:Plan)-[:HAS_PLAN_TREE]->(planTree:PlanTree)
OPTIONAL MATCH (m)<-[:ANOMALY_FOR]-(anomaly:Anomaly)
WITH project.owner AS neo4jOwner,
     project.branch AS neo4jBranch,
     project.version AS neo4jVersion,
     b.simple_name AS benchmarkSimpleName,
     b.description AS benchmarkDescription,
     b.name AS benchmarkName,
     b.mode AS benchmarkMode,
     b.cypher_query AS query,
     tr.date AS date,
     tr.build AS build,
     tr.archive AS testRunArchive,
     testRunAnnotations,
     tr.parent_build AS parentBuild,
     tr.triggered_by AS triggeredBy,
     project.commit AS commit,
     project.name AS projectName,
     project.owner AS projectOwner,
     m.unit AS unit,
     m.mean AS mean,
     e.operating_system AS os,
     e.server AS server,
     java {.*} AS java,
     m.error AS error,
     tr.id AS testRunId,
     collect(metricsAnnotation {.*}) AS metricsAnnotations,
     bt.repository_name AS repositoryName,
     btv.commit AS toolCommit,
     btv.owner AS toolOwner,
     btv.branch AS toolBranch,
     bt.name AS toolName,
     planTree.description_hash AS planHash,
     profiles {.*} AS profiles,
     auxiliaryMetrics {.*} AS auxiliaryMetrics,
     anomaly {.*} AS anomaly,
     neo4jConfig {.*} AS neo4jConfig,
     plan.used_runtime AS usedRuntime
ORDER BY date
RETURN neo4jOwner,
       neo4jBranch,
       neo4jVersion,
       benchmarkSimpleName,
       benchmarkName,
       benchmarkMode,
       triggeredBy,
       collect([commit,
                date,
                unit,
                mean,
                error,
                testRunId,
                build,
                toolCommit,
                testRunArchive,
                metricsAnnotations,
                testRunAnnotations,
                profiles,
                os,
                server,
                java,
                parentBuild,
                toolName,
                planHash,
                toolBranch,
                toolOwner,
                benchmarkDescription,
                projectName,
                projectOwner,
                repositoryName,
                anomaly,
                triggeredBy,
                neo4jConfig,
                query,
                auxiliaryMetrics,
                usedRuntime]) AS dateOrderedDataPoints
ORDER BY neo4jVersion