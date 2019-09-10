/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.io.IOException;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StoreId;

public class StoreDownloadContext
{
    private final Database database;
    private final StoreFiles storeFiles;
    private final LogFiles transactionLogs;
    private final Log log;
    private final ClusterInternalDbmsOperator internalOperator;

    private volatile StoreId storeId;

    //TODO: Merge this and ReadReplicaDatabaseContext into StoreCopyContext
    public StoreDownloadContext( Database database, StoreFiles storeFiles, LogFiles transactionLogs, ClusterInternalDbmsOperator internalOperator )
    {
        this.database = database;
        this.storeFiles = storeFiles;
        this.transactionLogs = transactionLogs;
        this.log = database.getInternalLogProvider().getLog( getClass() );
        this.internalOperator = internalOperator;
    }

    NamedDatabaseId databaseId()
    {
        return database.getNamedDatabaseId();
    }

    DatabaseLayout databaseLayout()
    {
        return database.getDatabaseLayout();
    }

    public StoreId storeId()
    {
        if ( storeId == null )
        {
            storeId = readStoreIdFromDisk();
        }
        return storeId;
    }

    private StoreId readStoreIdFromDisk()
    {
        try
        {
            return storeFiles.readStoreId( databaseLayout() );
        }
        catch ( IOException e )
        {
            log.warn( "Failure reading store id", e );
            return null;
        }
    }

    boolean isEmpty()
    {
        return storeFiles.isEmpty( databaseLayout() );
    }

    void delete() throws IOException
    {
        storeFiles.delete( databaseLayout(), transactionLogs );
    }

    ClusterInternalDbmsOperator.StoreCopyHandle stopForStoreCopy()
    {
        return internalOperator.stopForStoreCopy( database.getNamedDatabaseId() );
    }

    public Database database()
    {
        return database;
    }
}
