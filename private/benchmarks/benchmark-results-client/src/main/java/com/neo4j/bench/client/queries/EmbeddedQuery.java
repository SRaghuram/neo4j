/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import org.neo4j.graphdb.GraphDatabaseService;

public interface EmbeddedQuery<RESULT>
{
    RESULT execute( GraphDatabaseService db );
}
