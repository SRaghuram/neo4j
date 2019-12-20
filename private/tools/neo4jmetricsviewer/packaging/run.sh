docker container rm --force neo4j-metrics-viewer &> /dev/null
docker load -i neo4j-metrics-viewer.tar.gz
docker container run --publish 3000:3000 --detach --name neo4j-metrics-viewer neo4j-metrics-viewer:1.0
docker cp metrics neo4j-metrics-viewer:/src/
docker exec neo4j-metrics-viewer node builddashboards.js --loglevel=error
docker exec neo4j-metrics-viewer npm -s install fastify@2.11.0 csvtojson@2.0.10  &> /dev/null
docker exec neo4j-metrics-viewer node --max_old_space_size=8000 csvtojsonserver.js --loglevel=error &
echo 'Grafana should be available at http://localhost:3000/d/neo4j-metrics/'