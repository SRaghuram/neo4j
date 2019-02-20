MATCH (n:PROFILES)
RETURN min(n.children), max(n.children), count(n.children), count(DISTINCT n.children)
