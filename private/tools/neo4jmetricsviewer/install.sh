docker image build -t neo4j-metrics-viewer:1.0 .
docker save neo4j-metrics-viewer | gzip > packaging/neo4j-metrics-viewer.tar.gz
docker container rm --force neo4j-metrics-viewer

#docker exec -it neo4j-metrics-viewer /bin/bash