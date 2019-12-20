docker image build -t neo4j-metrics-viewer:1.0 . -q
docker save neo4j-metrics-viewer | gzip > packaging/neo4j-metrics-viewer.tar.gz
echo 'Created docker image at packaging/neo4j-metrics-viewer.tar.gz'