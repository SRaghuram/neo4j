MATCH
    (tr:TestRun)-[:HAS_METRICS]->(m:Metrics),
    (tr)-[:HAS_BENCHMARK_CONFIG]->(bc:BenchmarkConfig),
    (tr)-[:HAS_CONFIG]->(nc1:Neo4jConfig),
    (m)-[:HAS_CONFIG]->(nc2:Neo4jConfig)
WHERE tr.id='6cc6ef4c-9422-4c06-8198-0033d795d433'
OPTIONAL MATCH
    (tr)-[:WITH_ANNOTATION]->(a1:Annotation)
OPTIONAL MATCH
    (m)-[:WITH_ANNOTATION]->(a2:Annotation)
OPTIONAL MATCH
    (m)-[:HAS_PROFILES]->(p:Profiles)
DETACH DELETE tr,m,bc,nc1,nc2,a1,a2,p
