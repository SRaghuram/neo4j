MATCH (a:PROFILES), (b:PROFILES)
WHERE
    a.pets STARTS WITH "x" AND
    b.children STARTS WITH "x" AND
    a.pets = b.children
RETURN id(a), id(b)
