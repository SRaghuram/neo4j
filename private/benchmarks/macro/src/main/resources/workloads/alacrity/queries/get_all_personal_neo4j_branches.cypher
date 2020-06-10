MATCH (p:Project)<-[:WITH_PROJECT]-(tr:TestRun)
WHERE tr.triggered_by <> 'neo4j' AND p.owner <> 'opencypher'
WITH DISTINCT p.owner AS owner, p.branch AS branch, tr.triggered_by AS trigger
WITH owner, toLower(branch) AS branch, trigger
RETURN DISTINCT owner, branch, trigger
ORDER BY owner, branch, trigger