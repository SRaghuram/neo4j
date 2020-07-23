MATCH (logicalInterface:LogicalInterface)
  WHERE NOT (logicalInterface:Model)
  AND $latest IN logicalInterface.latest
WITH logicalInterface
OPTIONAL MATCH (logicalInterface)<-[:HAS_CONNECTION_COMPONENT{isEndOfConnection:'Y'}]-(logicalConnection:LogicalConnection)
  WHERE NOT (logicalConnection:Model)
  AND $latest IN logicalConnection.latest
WITH logicalInterface, logicalConnection
OPTIONAL MATCH (logicalConnection)-[:HAS_CONNECTION_COMPONENT{isEndOfConnection:'Y'}]->(simpleLogicalInterface:LogicalInterface)
  WHERE NOT(simpleLogicalInterface = logicalInterface)
  AND NOT (simpleLogicalInterface:Model)
  AND $latest IN simpleLogicalInterface.latest
WITH logicalInterface, logicalConnection, simpleLogicalInterface
OPTIONAL MATCH (service:Service)-[:HAS_LOGICAL]->(logicalConnection)
  WHERE NOT(service:Model)
  AND ANY (p IN service.latest WHERE p IN [$latest,11])
WITH logicalInterface, logicalConnection, simpleLogicalInterface, service
OPTIONAL MATCH (org:Organization)-[:HAS_LOGICAL]-> (service)
  WHERE NOT(org:Model)
  AND ANY (p IN org.latest WHERE p IN [$latest,11])
WITH  DISTINCT  logicalInterface, logicalConnection, simpleLogicalInterface, service, org
RETURN count(*)
