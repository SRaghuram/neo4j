/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.OutputStream;
import java.util.function.Supplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Objects.requireNonNull;
import static org.neo4j.backup.impl.BackupPageCacheContainer.of;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

public final class BackupProtocolServiceFactory
{

    private BackupProtocolServiceFactory()
    {
    }

    public static BackupProtocolService backupProtocolService( OutputStream logDestination, Config config )
    {
        JobScheduler scheduler = createInitialisedScheduler();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PageCache pageCache = createPageCache( fs, config, scheduler );
        BackupPageCacheContainer pageCacheContainer = of( pageCache, scheduler );
        return backupProtocolService( () -> fs, toOutputStream( logDestination ), logDestination, new Monitors(), pageCacheContainer );
    }

    public static BackupProtocolService backupProtocolService( Supplier<FileSystemAbstraction> fileSystemSupplier, LogProvider logProvider,
            OutputStream logDestination, Monitors monitors, PageCache pageCache )
    {
        return backupProtocolService( fileSystemSupplier, logProvider, logDestination, monitors, of( pageCache ) );
    }

    private static BackupProtocolService backupProtocolService( Supplier<FileSystemAbstraction> fileSystemSupplier, LogProvider logProvider,
            OutputStream logDestination, Monitors monitors, BackupPageCacheContainer pageCacheContainer )
    {
        requireNonNull( fileSystemSupplier );
        requireNonNull( logProvider );
        requireNonNull( logDestination );
        requireNonNull( monitors );
        requireNonNull( pageCacheContainer );
        return new BackupProtocolService( fileSystemSupplier, logProvider, logDestination, monitors, pageCacheContainer );
    }
}
