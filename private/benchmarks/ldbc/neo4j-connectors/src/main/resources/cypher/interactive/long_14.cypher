MATCH path = allShortestPaths((person1:Person {id:{1}})-[:KNOWS*0..]-(person2:Person {id:{2}}))
RETURN
 extract(n IN nodes(path) | n.id) AS pathNodeIds,
 reduce(weight=0.0, r IN rels(path) |
            weight +
            length(()-[r]->()<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->()-[r]->())*1.0 +
            length(()<-[r]-()<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->()<-[r]-())*1.0 +
            length(()<-[r]-()-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_COMMENT]-(:Comment)-[:COMMENT_HAS_CREATOR]-()<-[r]-())*0.5
 ) AS weight
ORDER BY weight DESC
