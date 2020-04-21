/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.dbms.EnterpriseSystemGraphComponent;

import org.neo4j.configuration.Config;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;

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
