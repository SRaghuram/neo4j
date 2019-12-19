docker container rm --force neo4j-metrics-viewer
docker load -i neo4j-metrics-viewer.tar.gz
docker container run --publish 3000:3000 --detach --name neo4j-metrics-viewer neo4j-metrics-viewer:1.0
docker cp metrics neo4j-metrics-viewer:/src/
docker exec neo4j-metrics-viewer node builddashboards.js
docker exec neo4j-metrics-viewer npm install fastify@2.11.0 csvtojson@2.0.10
docker exec neo4j-metrics-viewer node csvtojsonserver.js &
echo 'Grafana should be available at http://localhost:3000/'