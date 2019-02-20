MATCH (toFrom:Entity { Id: { eId }}),(segment)-[:precedes|is]->(startSegment),(toFrom)-[toFrom_rel:read|wrote]->(startSegment)
WITH segment
MATCH (segment)-[:regarding_summary_phrase]->(entity)
WITH segment, entity
MATCH (checkIgnoredWriter)-[:wrote]->(segment)
WHERE ((checkIgnoredWriter.Rank > 0 OR checkIgnoredWriter.Rank IS NULL )) AND ((entity.Rank > 0 OR entity.Rank IS NULL )) AND (NOT (entity.Id = { eId }))
RETURN entity.Id AS Id, entity.Name AS Name, entity.Type AS Type, entity.MentionNames AS Mentions, count(DISTINCT segment) AS SegmentCount
ORDER BY SegmentCount DESC SKIP 0
LIMIT 51