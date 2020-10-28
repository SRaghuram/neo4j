MATCH (test_run:TestRun { id : $testRunId } )-[:WITH_TOOL]->(:BenchmarkToolVersion)-[:VERSION_OF]->(benchmark_tool:BenchmarkTool)
UNWIND $errors as error
WITH test_run,
     benchmark_tool,
     error.benchmarkGroupName AS benchmarkGroupName,
     error.benchmarkProperties.name AS benchmarkName,
     error.benchmarkProperties.description AS benchmarkDescription,
     error.benchmarkProperties.cypher_query AS cypherQuery,
     error.benchmarkProperties AS benchmarkProperties,
     error.benchmarkParams AS benchmarkParams,
     error.message AS errorMessage
MERGE (benchmark_group:BenchmarkGroup {name: benchmarkGroupName})<-[:IMPLEMENTS]-(benchmark_tool)
MERGE (benchmark_group)-[:HAS_BENCHMARK]->(benchmark:Benchmark {name: benchmarkName})-[:HAS_PARAMS]->(params:BenchmarkParams)
// Is new benchmark
  ON CREATE SET
  benchmark=benchmarkProperties,
  params=benchmarkParams
// Description & Query overwritten on purpose. It may get updated/corrected over time, database should reflect these changes
  ON MATCH SET
  benchmark.description=benchmarkDescription,
  benchmark.cypher_query=cypherQuery
CREATE (benchmark)<-[:ERROR_FOR]-(test_run_error:Error { message : errorMessage } )<-[:HAS_ERROR]-(test_run)
RETURN test_run, collect(test_run_error) as test_run_errors