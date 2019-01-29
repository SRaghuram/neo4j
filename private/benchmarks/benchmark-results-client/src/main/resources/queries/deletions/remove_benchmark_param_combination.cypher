MATCH
    (bg:BenchmarkGroup)-[:HAS_BENCHMARK]->(b:Benchmark),
    (b)-[:HAS_PARAMS]->(bp:BenchmarkParams)
WHERE bg.name='LdbcSnbInteractive-Read' AND
    bp.api='EMBEDDED_CORE' AND
    bp.scale_factor='10'
DETACH DELETE b,bp
