MATCH (city:City {id:{2}})
OPTIONAL MATCH (tag:Tag) WHERE tag.id IN {3}
WITH city, collect(tag) AS tags
CREATE (person:Person {1})-[:PERSON_IS_LOCATED_IN]->(city)
FOREACH (tag IN tags | CREATE (person)-[:HAS_INTEREST]->(tag))
FOREACH (studyAt IN {4} |
   MERGE (university:University {id:studyAt[0]})
   CREATE (person)-[:STUDY_AT {classYear:studyAt[1]}]->(university))
FOREACH (workAt IN {5} |
   MERGE (company:Company {id:workAt[0]})
   CREATE (person)-[:WORKS_AT {workFrom:workAt[1]}]->(company))
