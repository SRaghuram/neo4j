MATCH  (tr:TestRun {id: {test_run_id}})
CREATE (tr)-[:WITH_ANNOTATION]->(a:Annotation {annotation})
RETURN a