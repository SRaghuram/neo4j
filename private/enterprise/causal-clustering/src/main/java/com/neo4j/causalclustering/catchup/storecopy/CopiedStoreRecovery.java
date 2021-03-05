/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.helper.StoreValidation;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

public class CopiedStoreRecovery extends LifecycleAdapter
{
    private static final String STORE_RECOVERY_VERSION_CHECKER_TAG = "storeRecoveryVersionChecker";
    private final PageCache pageCache;
    private final DatabaseTracers databaseTracers;
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactory;
    private final MemoryTracker memoryTracker;

    private boolean shutdown;

    public CopiedStoreRecovery( PageCache pageCache, DatabaseTracers databaseTracers, FileSystemAbstraction fs, StorageEngineFactory storageEngineFactory,
            MemoryTracker memoryTracker )
    {
        this.pageCache = pageCache;
        this.databaseTracers = databaseTracers;
        this.fs = fs;
        this.storageEngineFactory = storageEngineFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown = true;
    }

    public synchronized boolean canRecoverRemoteStore( Config config, DatabaseLayout databaseLayout, StoreVersion remoteStoreVersion )
    {
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck( fs, databaseLayout, config, pageCache, NullLogService.getInstance(),
                databaseTracers.getPageCacheTracer() );
        return canRecoverRemoteStore( storeVersionCheck, remoteStoreVersion );
    }

    public synchronized void recoverCopiedStore( Config config, DatabaseLayout databaseLayout ) throws DatabaseShutdownException, IOException
    {
        if ( shutdown )
        {
            throw new DatabaseShutdownException( "Abort store-copied store recovery due to database shutdown" );
        }

        var pageCacheTracer = databaseTracers.getPageCacheTracer();
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck( fs, databaseLayout, config, pageCache, NullLogService.getInstance(),
                pageCacheTracer );
        Optional<String> storeVersion = getStoreVersion( storeVersionCheck, pageCacheTracer );
        if ( storeVersion.isPresent() )
        {
            //It is ok to have recover an older version of the store. In that case we will migrate it after recovery. Minor migrations are fast
            if ( !canRecoverRemoteStore( storeVersionCheck, storeVersionCheck.versionInformation( storeVersion.get() ) ) )
            {
                throw new RuntimeException( failedToStartMessage( config ) );
            }
        }

        try
        {
            performRecovery( fs, pageCache, databaseTracers, config, databaseLayout, storageEngineFactory, memoryTracker );
        }
        catch ( Exception e )
        {
            if ( indexOfThrowable( e, UpgradeNotAllowedException.class ) != -1 )
            {
                throw new RuntimeException( failedToStartMessage( config ), e );
            }
            throw e;
        }
    }

    private boolean canRecoverRemoteStore( StoreVersionCheck storeVersionCheck, StoreVersion remoteStoreVersion )
    {
        String storeVersion = storeVersionCheck.configuredVersion(); //When can this be null? Kept for compatibility reasons
        return storeVersion == null || StoreValidation.validRemoteToUseStoreFrom( storageEngineFactory.versionInformation( storeVersion ), remoteStoreVersion );
    }

    private Optional<String> getStoreVersion( StoreVersionCheck storeVersionCheck, PageCacheTracer pageCacheTracer )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_RECOVERY_VERSION_CHECKER_TAG ) )
        {
            return storeVersionCheck.storeVersion( cursorTracer );
        }
    }

    private String failedToStartMessage( Config config )
    {
        String recordFormat = config.get( record_format );

        return format( "Failed to start database with copied store. This may be because the core servers and " +
                        "read replicas have a different record format. On this machine: `%s=%s`. Check the equivalent" +
                        " value on the core server.", record_format.name(), recordFormat );
    }
}
