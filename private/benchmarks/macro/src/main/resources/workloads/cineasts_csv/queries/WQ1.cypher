LOAD CSV WITH HEADERS FROM {csv_filename} AS line
CREATE (:User {name: line.name, login: line.login, password: line.password})
