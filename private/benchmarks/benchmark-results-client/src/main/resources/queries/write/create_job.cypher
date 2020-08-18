CREATE (job:Job $job)
WITH job
MATCH (tr:TestRun)
  WHERE tr.id = $testRunId
CREATE (job)<-[:RUN_BY]-(tr)
