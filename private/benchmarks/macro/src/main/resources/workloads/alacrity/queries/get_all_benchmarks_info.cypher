MATCH (b:Benchmark)<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup)<-[:IMPLEMENTS]-(bt:BenchmarkTool)
WHERE (bt)<-[:VERSION_OF]-(:BenchmarkToolVersion)<-[:WITH_TOOL]-(:TestRun) AND
      b.active = true
RETURN bg.name AS benchmarkGroupName,
       collect([b.name, b.simple_name]) AS benchmarksInfo,
       bt.name + ' : ' + bg.name AS benchAndToolName
ORDER by benchAndToolName