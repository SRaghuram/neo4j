/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class TemporaryDatabase implements AutoCloseable
{
    private final DatabaseManagementService managementService;
    private final GraphDatabaseAPI database;

    public TemporaryDatabase( DatabaseManagementService managementService, boolean isSystem )
    {
        String databaseName = isSystem ? SYSTEM_DATABASE_NAME : DEFAULT_DATABASE_NAME;
        this.managementService = managementService;
        this.database = (GraphDatabaseAPI) managementService.database( databaseName );
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return database;
    }

    public DatabaseLayout databaseDirectory()
    {
        return database.databaseLayout();
    }

    @Override
    public void close()
    {
        managementService.shutdown();
    }
}
