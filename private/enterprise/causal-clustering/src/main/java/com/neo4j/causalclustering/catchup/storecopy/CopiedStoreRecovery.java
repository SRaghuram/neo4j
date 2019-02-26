/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

public class CopiedStoreRecovery extends LifecycleAdapter
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final StorageEngineFactory storageEngineFactory;

    private boolean shutdown;

    public CopiedStoreRecovery( PageCache pageCache, FileSystemAbstraction fs, StorageEngineFactory storageEngineFactory )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.storageEngineFactory = storageEngineFactory;
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown = true;
    }

    public synchronized void recoverCopiedStore( Config config, DatabaseLayout databaseLayout ) throws DatabaseShutdownException, IOException
    {
        if ( shutdown )
        {
            throw new DatabaseShutdownException( "Abort store-copied store recovery due to database shutdown" );
        }

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( config, pageCache, fs, NullLogService.getInstance(), databaseLayout );
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck( dependencies );
        Optional<String> storeVersion = storeVersionCheck.storeVersion();
        if ( storeVersion.isPresent() )
        {
            StoreVersion version = storeVersionCheck.versionInformation( storeVersion.get() );
            String configuredVersion = storeVersionCheck.configuredVersion();
            if ( configuredVersion != null && !version.isCompatibleWith( storeVersionCheck.versionInformation( configuredVersion ) ) )
            {
                throw new RuntimeException( failedToStartMessage( config ) );
            }
        }

        try
        {
            performRecovery( fs, pageCache, config, databaseLayout, storageEngineFactory );
        }
        catch ( Exception e )
        {
            Throwable peeled = Exceptions.peel( e, t -> !(t instanceof UpgradeNotAllowedException) );
            if ( peeled != null )
            {
                throw new RuntimeException( failedToStartMessage( config ), e );
            }
            else
            {
                throw e;
            }
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
