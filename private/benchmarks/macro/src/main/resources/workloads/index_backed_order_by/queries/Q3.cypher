MATCH (n:PROFILES) WHERE n.pets > "r" RETURN sum(size(n.pets)), size(n.eye_color)
