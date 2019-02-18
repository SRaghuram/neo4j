/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.neo4j.graphdb.GraphDatabaseService;

public class TemporaryDatabase implements AutoCloseable
{
    private final GraphDatabaseService graphDatabaseService;

    public TemporaryDatabase( GraphDatabaseService graphDatabaseService )
    {
        this.graphDatabaseService = graphDatabaseService;
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return graphDatabaseService;
    }

    @Override
    public void close()
    {
        graphDatabaseService.shutdown();
    }
}

