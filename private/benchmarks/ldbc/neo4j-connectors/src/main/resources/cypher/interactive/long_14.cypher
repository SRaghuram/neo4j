MATCH path = allShortestPaths((person1:Person {id:$1})-[:KNOWS*0..]-(person2:Person {id:$2}))
WITH nodes(path) AS pathNodes
CALL {
  WITH pathNodes
  UNWIND range(0, size(pathNodes) - 2) AS i
  WITH pathNodes[i] AS nodeA,
       pathNodes[i+1] AS nodeB
  CALL {
      WITH nodeA, nodeB
      MATCH (nodeA)<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->(nodeB)
      RETURN count(*) * 1.0 AS weight // direct replies (by nodeA) to a Post (by nodeB)
    UNION ALL
      WITH nodeA, nodeB
      MATCH (nodeB)<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->(nodeA)
      RETURN count(*) * 1.0 AS weight // count direct replies (by nodeA) to a Post (by nodeB)
    UNION ALL
      WITH nodeA, nodeB
      MATCH (nodeA)-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_COMMENT]-(:Comment)-[:COMMENT_HAS_CREATOR]-(nodeB)
      RETURN count(*) * 0.5 AS weight // count direct replies (by one of the nodes) to a Comment (by the other node)
  }
  RETURN sum(weight) AS weight
}
RETURN [n IN pathNodes | n.id] AS pathNodeIds, weight
ORDER BY weight DESC
