/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static java.lang.String.format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.isStoreAndConfigFormatsCompatible;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

public class CopiedStoreRecovery extends LifecycleAdapter
{
    private final Config config;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;

    private boolean shutdown;

    public CopiedStoreRecovery( Config config, PageCache pageCache, FileSystemAbstraction fs )
    {
        this.config = config;
        this.pageCache = pageCache;
        this.fs = fs;
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown = true;
    }

    public synchronized void recoverCopiedStore( DatabaseLayout databaseLayout ) throws DatabaseShutdownException, IOException
    {
        if ( shutdown )
        {
            throw new DatabaseShutdownException( "Abort store-copied store recovery due to database shutdown" );
        }
        if ( !isStoreAndConfigFormatsCompatible( config, databaseLayout, fs, pageCache, NullLogProvider.getInstance() ) )
        {
            throw new RuntimeException( failedToStartMessage() );
        }

        try
        {
            performRecovery( fs, pageCache, config, databaseLayout );
        }
        catch ( Exception e )
        {
            Throwable peeled = Exceptions.peel( e, t -> !(t instanceof UpgradeNotAllowedException) );
            if ( peeled != null )
            {
                throw new RuntimeException( failedToStartMessage(), e );
            }
            else
            {
                throw e;
            }
        }
    }

    private String failedToStartMessage()
    {
        String recordFormat = config.get( record_format );

        return format( "Failed to start database with copied store. This may be because the core servers and " +
                        "read replicas have a different record format. On this machine: `%s=%s`. Check the equivalent" +
                        " value on the core server.", record_format.name(), recordFormat );
    }
}
