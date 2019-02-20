MATCH (toFrom:Entity { Id: { eId }}),(toFrom)-[toFrom_rel:read|wrote]->(segment:Segment),(copy)-[:of]->(document)-[documentSegment:containing]->(segment),(segment)-[:in_thread]->(thread:Thread),(checkIgnoredWriter:Entity)-[:wrote]->(segment)
WHERE (checkIgnoredWriter.Rank > 0 OR checkIgnoredWriter.Rank IS NULL )
RETURN count(DISTINCT thread) AS ThreadCount, count(DISTINCT segment) AS SegmentCount
SKIP 0