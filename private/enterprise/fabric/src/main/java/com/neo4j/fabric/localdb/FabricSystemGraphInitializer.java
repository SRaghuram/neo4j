/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.dbms.EnterpriseSystemGraphInitializer;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;

public class FabricSystemGraphInitializer extends EnterpriseSystemGraphInitializer
{
    private final FabricDatabaseManager fabricDatabaseManager;

    public FabricSystemGraphInitializer( DatabaseManager<?> databaseManager, Config config,
            FabricDatabaseManager fabricDatabaseManager )
    {
        super( databaseManager, config );
        this.fabricDatabaseManager = fabricDatabaseManager;
    }

    @Override
    protected void manageDatabases( GraphDatabaseService system, boolean update )
    {
        fabricDatabaseManager.manageFabricDatabases( system, update );
    }
}
