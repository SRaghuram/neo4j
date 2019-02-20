MATCH (n:PROFILES)
WHERE n.pets > ""
RETURN max(n.pets)
