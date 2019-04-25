/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import java.io.File;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TemporaryDatabase implements AutoCloseable
{
    private final DatabaseManagementService managementService;
    private final GraphDatabaseAPI defaultDatabase;

    public TemporaryDatabase( DatabaseManagementService managementService )
    {
        this.managementService = managementService;
        this.defaultDatabase = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return defaultDatabase;
    }

    public File defaultDatabaseDirectory()
    {
        return defaultDatabase.databaseLayout().databaseDirectory();
    }

    @Override
    public void close()
    {
        managementService.shutdown();
    }
}

