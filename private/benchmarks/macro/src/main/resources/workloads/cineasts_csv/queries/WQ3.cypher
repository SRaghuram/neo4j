USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM {csv_filename} AS line
  CREATE (m:Movie {id: line.id})
  SET m = line
  SET m.version = toInt(line.version)
  SET m.lastModified = toInt(line.lastModified)
  SET m.releaseDate = toInt(line.releaseDate)
  SET m.runtime = toInt(line.runtime)
  SET m.tagline = coalesce(line.tagline, '')
  SET m.homepage = coalesce(line.homepage, '')
  SET m.language = coalesce(line.language, '')
  SET m.description = coalesce(line.description, '')
