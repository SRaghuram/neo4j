MATCH (n0:Person),
      (n1:Person),
      (n2:Person),
      (n3:Person),
      (n4:Person),
      (n5:Person),
      (n6:Person),
      (n7:Person),
      (n8:Person),
      (n9:Person)
WHERE n0.row < 2 AND n0.col < 2
  AND n1.row < 2 AND n1.col < 2
  AND n2.row < 2 AND n2.col < 2
  AND n3.row < 2 AND n3.col < 2
  AND n4.row < 2 AND n4.col < 2
  AND n5.row < 2 AND n5.col < 2
  AND n6.row < 2 AND n6.col < 2
  AND n7.row < 2 AND n7.col < 2
  AND n8.row < 2 AND n8.col < 2
  AND n9.row < 2 AND n9.col < 2
  AND n0.row = n5.row
  AND n1.row = n6.row
  AND n2.row = n7.row
  AND n3.row = n8.row
  AND n4.row = n9.row
  AND n0.col = n1.col
  AND n2.col = n3.col
  AND n4.col = n5.col
  AND n6.col = n7.col
  AND n8.col = n9.col
RETURN count(*)