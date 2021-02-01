MATCH (benchmark:Benchmark)
OPTIONAL MATCH (benchmark_group:BenchmarkGroup)-[:HAS_BENCHMARK]->(benchmark)
OPTIONAL MATCH (metrics:Metrics)-[:METRICS_FOR]->(benchmark)
RETURN benchmark_group, benchmark, count(metrics) AS metrics_count
