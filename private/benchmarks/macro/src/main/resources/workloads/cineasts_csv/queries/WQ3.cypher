USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM {csv_filename} AS line
  CREATE (m:Movie {id: line.id})
  SET m = line
  SET m.version = toInteger(line.version)
  SET m.lastModified = toInteger(line.lastModified)
  SET m.releaseDate = toInteger(line.releaseDate)
  SET m.runtime = toInteger(line.runtime)
  SET m.tagline = coalesce(line.tagline, '')
  SET m.homepage = coalesce(line.homepage, '')
  SET m.language = coalesce(line.language, '')
  SET m.description = coalesce(line.description, '')
