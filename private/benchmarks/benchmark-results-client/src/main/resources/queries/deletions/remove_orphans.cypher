MATCH (n)
WHERE (n:Project OR n:BenchmarkTool OR n:Java OR n:Environment) AND
      size((n)--())=0
DETACH DELETE n
