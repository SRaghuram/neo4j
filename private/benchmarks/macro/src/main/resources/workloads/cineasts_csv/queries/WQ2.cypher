USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM {csv_filename} AS line
  CREATE (p:Person {id: line.id})
  SET p = line
  SET p.lastModified = toInt(line.lastModified)
  SET p.version = toInt(line.version)
