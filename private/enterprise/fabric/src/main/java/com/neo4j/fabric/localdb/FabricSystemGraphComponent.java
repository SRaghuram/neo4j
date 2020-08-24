/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.dbms.EnterpriseSystemGraphComponent;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;

/***
 * This is an enterprise component for databases.
 * It is building on {@link DefaultSystemGraphComponent}, with the only differences being that:
 * 1. The old default database is not stopped when a new default database is chosen.
 * 2. An extra post initialization step for fabric is performed for single instance enterprise edition.
 */
public class FabricSystemGraphComponent extends EnterpriseSystemGraphComponent
{
    private final FabricDatabaseManager fabricDatabaseManager;

    public FabricSystemGraphComponent( Config config, FabricDatabaseManager fabricDatabaseManager )
    {
        super( config );
        this.fabricDatabaseManager = fabricDatabaseManager;
    }

    @Override
    protected void postInitialization( GraphDatabaseService system, boolean wasInitialized )
    {
        fabricDatabaseManager.manageFabricDatabases( system, !wasInitialized );
    }
}
