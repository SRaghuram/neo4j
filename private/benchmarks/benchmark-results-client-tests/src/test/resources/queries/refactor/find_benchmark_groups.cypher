MATCH (benchmark_group:BenchmarkGroup)
OPTIONAL MATCH (benchmark_tool:BenchmarkTool)-[:IMPLEMENTS]->(benchmark_group)
RETURN benchmark_tool, benchmark_group
