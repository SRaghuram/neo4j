/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import com.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.transaction.TransactionManager;

import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.availability.UnavailableException;

import static java.lang.String.format;

public class BoltFabricDatabaseManagementService implements BoltGraphDatabaseManagementServiceSPI
{
    private final FabricBookmarkParser fabricBookmarkParser = new FabricBookmarkParser();
    private final FabricExecutor fabricExecutor;
    private final FabricConfig config;
    private final TransactionManager transactionManager;
    private final FabricDatabaseManager fabricDatabaseManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final TransactionBookmarkManagerFactory transactionBookmarkManagerFactory;

    public BoltFabricDatabaseManagementService( FabricExecutor fabricExecutor, FabricConfig config, TransactionManager transactionManager,
            FabricDatabaseManager fabricDatabaseManager, LocalGraphTransactionIdTracker transactionIdTracker,
            TransactionBookmarkManagerFactory transactionBookmarkManagerFactory )
    {
        this.fabricExecutor = fabricExecutor;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricDatabaseManager = fabricDatabaseManager;
        this.transactionIdTracker = transactionIdTracker;
        this.transactionBookmarkManagerFactory = transactionBookmarkManagerFactory;
    }

    @Override
    public BoltGraphDatabaseServiceSPI database( String databaseName ) throws UnavailableException, DatabaseNotFoundException
    {
        try
        {
            var database = fabricDatabaseManager.getDatabase( databaseName );
            return new BoltFabricDatabaseService( database.databaseId(), fabricExecutor, config, transactionManager, transactionIdTracker,
                    transactionBookmarkManagerFactory );
        }
        catch ( DatabaseShutdownException | UnavailableException e )
        {
            throw new UnavailableException( format( "Database '%s' is unavailable.", databaseName ) );
        }

    }

    @Override
    public Optional<CustomBookmarkFormatParser> getCustomBookmarkFormatParser()
    {
        return Optional.of( fabricBookmarkParser );
    }
}
