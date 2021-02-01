MATCH (benchmark_tool:BenchmarkTool {name: $benchmark_tool_name})
        -[:IMPLEMENTS]->(:BenchmarkGroup {name: $old_benchmark_group_name})
        -[:HAS_BENCHMARK]->(old_benchmark:Benchmark {simple_name: $benchmark_simple_name})
        -[:HAS_PARAMS]->(old_params:BenchmarkParams),
      (old_benchmark)<-[:METRICS_FOR]-(metrics:Metrics)
MERGE (benchmark_tool)-[:IMPLEMENTS]->(new_benchmark_group:BenchmarkGroup {name: $new_benchmark_group_name})
MERGE (new_benchmark_group)-[:HAS_BENCHMARK]->(new_benchmark:Benchmark {name: old_benchmark.name})
  ON CREATE SET new_benchmark = old_benchmark
MERGE (new_benchmark)<-[:METRICS_FOR]-(metrics)
MERGE (new_benchmark)-[:HAS_PARAMS]->(new_params:BenchmarkParams)
  ON CREATE SET new_params = old_params
DETACH DELETE old_params, old_benchmark
RETURN new_benchmark
