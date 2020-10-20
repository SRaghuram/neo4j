/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.io.IOException;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StoreId;

public class StoreDownloadContext
{
    private static final String STORE_DOWNLOAD_STORE_ID_READER_TAG = "storeDownloadStoreIdReader";
    private final Database kernelDatabase;
    private final StoreFiles storeFiles;
    private final LogFiles transactionLogs;
    private final Log log;
    private final ClusterInternalDbmsOperator internalOperator;
    private final PageCacheTracer pageCacheTracer;

    private volatile StoreId storeId;

    //TODO: Merge this and ReadReplicaDatabaseContext into StoreCopyContext
    public StoreDownloadContext( Database kernelDatabase, StoreFiles storeFiles, LogFiles transactionLogs, ClusterInternalDbmsOperator internalOperator,
            PageCacheTracer pageCacheTracer )
    {
        this.kernelDatabase = kernelDatabase;
        this.storeFiles = storeFiles;
        this.transactionLogs = transactionLogs;
        this.log = kernelDatabase.getInternalLogProvider().getLog( getClass() );
        this.internalOperator = internalOperator;
        this.pageCacheTracer = pageCacheTracer;
    }

    NamedDatabaseId databaseId()
    {
        return kernelDatabase.getNamedDatabaseId();
    }

    DatabaseLayout databaseLayout()
    {
        return kernelDatabase.getDatabaseLayout();
    }

    StoreId storeId()
    {
        if ( storeId == null )
        {
            storeId = readStoreIdFromDisk();
        }
        return storeId;
    }

    private StoreId readStoreIdFromDisk()
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_DOWNLOAD_STORE_ID_READER_TAG ) )
        {
            return storeFiles.readStoreId( databaseLayout(), cursorTracer );
        }
        catch ( IOException e )
        {
            log.warn( "Failure reading store id", e );
            return null;
        }
    }

    boolean hasStore()
    {
        return !storeFiles.isEmpty( databaseLayout() );
    }

    void delete() throws IOException
    {
        storeFiles.delete( databaseLayout(), transactionLogs );
    }

    ClusterInternalDbmsOperator.StoreCopyHandle stopForStoreCopy()
    {
        return internalOperator.stopForStoreCopy( kernelDatabase.getNamedDatabaseId() );
    }

    public Database kernelDatabase()
    {
        return kernelDatabase;
    }
}
