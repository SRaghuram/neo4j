/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.transaction.TransactionManager;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.availability.UnavailableException;

public class BoltFabricDatabaseManagementService implements BoltGraphDatabaseManagementServiceSPI
{
    private final FabricExecutor fabricExecutor;
    private final FabricConfig config;
    private final TransactionManager transactionManager;
    private final FabricDatabaseManager fabricDatabaseManager;

    public BoltFabricDatabaseManagementService( FabricExecutor fabricExecutor, FabricConfig config, TransactionManager transactionManager,
            FabricDatabaseManager fabricDatabaseManager )
    {
        this.fabricExecutor = fabricExecutor;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricDatabaseManager = fabricDatabaseManager;
    }

    @Override
    public BoltGraphDatabaseServiceSPI database( String databaseName ) throws UnavailableException, DatabaseNotFoundException
    {
        var  database = fabricDatabaseManager.getDatabase( databaseName );
        return new BoltFabricDatabaseService( database.databaseId(), fabricExecutor, config, transactionManager );
    }
}
