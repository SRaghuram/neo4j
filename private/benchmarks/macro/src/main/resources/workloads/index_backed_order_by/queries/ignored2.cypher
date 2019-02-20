// this is interesting if very few labels have :Rare, so it could be used to build a HashTable, and then
// we would range-scan the index on the rhs and retain the order
MATCH (a:PROFILES:Rare) WHERE exists(a.prop) RETURN a.foo ORDER BY a.prop
