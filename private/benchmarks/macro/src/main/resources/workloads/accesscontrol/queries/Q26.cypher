MATCH (:Administrator)-[r:MEMBER_OF]->()
RETURN count(r)