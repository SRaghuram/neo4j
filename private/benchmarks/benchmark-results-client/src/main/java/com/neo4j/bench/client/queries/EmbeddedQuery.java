package com.neo4j.bench.client.queries;

import org.neo4j.graphdb.GraphDatabaseService;

public interface EmbeddedQuery<RESULT>
{
    RESULT execute( GraphDatabaseService db );
}
