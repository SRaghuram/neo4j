MATCH  (:TestRun {id: {test_run_id}})-[:HAS_METRICS]->(metrics:Metrics),
       (metrics)-[:METRICS_FOR]->(benchmark:Benchmark {name: {benchmark_name}}),
       (benchmark)<-[:HAS_BENCHMARK]-(:BenchmarkGroup {name: {benchmark_group_name}})
CREATE (metrics)-[:HAS_PLAN]->(plan:Plan {plan})-[:HAS_COMPILATION_METRICS]->(:CompilationMetrics {compilation_metrics})
WITH plan
// MATCH instead of MERGE because PlanTree may not have been created. This query should not create PlanTree
MATCH (planTree:PlanTree {description_hash:{plan_description_hash}})
CREATE (plan)-[:HAS_PLAN_TREE]->(planTree)
