MATCH (n0:Person),
      (n1:Person),
      (n2:Person)
WHERE n0.row < 4 AND n0.col < 4
  AND n1.row < 4 AND n1.col < 4
  AND n2.row < 4 AND n2.col < 4
  AND n0.row + 3 = n1.row
  AND n1.col + 3 = n2.col
OPTIONAL MATCH (n0)-->(n0_1)-->(n0_2)
OPTIONAL MATCH (n1)-->(n1_1)-->(n1_2)
OPTIONAL MATCH (n2)-->(n2_1)-->(n2_2)
RETURN count(*)