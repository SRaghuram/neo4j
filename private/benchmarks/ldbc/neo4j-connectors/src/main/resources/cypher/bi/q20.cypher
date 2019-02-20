UNWIND $tagClasses AS tagClassName
MATCH
  (tagClass:TagClass {name: tagClassName})<-[:IS_SUBCLASS_OF*0..]-
  (:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message)
RETURN
  tagClass.name,
  count(message) AS postCount
ORDER BY
  postCount DESC,
  tagClass.name ASC
LIMIT 100
