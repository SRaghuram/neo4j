/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.com.storecopy.FileMoveProvider;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.lang.String.format;

public final class OnlineBackupExecutor
{
    private final FileSystemAbstraction fs;
    private final LogProvider userLogProvider;
    private final LogProvider internalLogProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final Monitors monitors;
    private final BackupSupportingClassesFactory backupSupportingClassesFactory;
    private final ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
    private final SystemNanoClock clock;

    private OnlineBackupExecutor( Builder builder )
    {
        this.fs = builder.fs;
        this.userLogProvider = builder.userLogProvider;
        this.internalLogProvider = builder.internalLogProvider;
        this.progressMonitorFactory = builder.progressMonitorFactory;
        this.monitors = builder.monitors;
        this.backupSupportingClassesFactory = builder.supportingClassesFactory;
        this.clock = builder.clock;
    }

    public static OnlineBackupExecutor buildDefault()
    {
        return builder().withClock( Clocks.nanoClock() ).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public void executeBackups( OnlineBackupContext.Builder contextBuilder ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        final var allDatabaseNames =
                getAllDatabaseNames( contextBuilder.getConfig(), contextBuilder.getDatabaseNamePattern(), contextBuilder.getAddress() );
        for ( OnlineBackupContext context : contextBuilder.build( allDatabaseNames ) )
        {
            executeBackup( context );
        }
    }

    private void executeBackup( OnlineBackupContext context ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        verify( context );

        try ( BackupSupportingClasses supportingClasses = backupSupportingClassesFactory.createSupportingClasses( context.getConfig() ) )
        {
            StoreCopyClientMonitor backupStoreCopyMonitor = new BackupOutputMonitor( userLogProvider, clock );
            monitors.addMonitorListener( backupStoreCopyMonitor );

            PageCache pageCache = supportingClasses.getPageCache();
            var pageCacheTracer = supportingClasses.getPageCacheTracer();

            StoreFiles storeFiles = new StoreFiles( fs, pageCache );
            BackupCopyService copyService = new BackupCopyService( fs, new FileMoveProvider( fs ), storeFiles, internalLogProvider, pageCacheTracer );

            BackupStrategy strategy = new DefaultBackupStrategy( supportingClasses.getBackupDelegator(), internalLogProvider, storeFiles, pageCacheTracer );
            BackupStrategyWrapper wrapper = new BackupStrategyWrapper( strategy, copyService, fs, pageCache, userLogProvider, internalLogProvider );

            BackupStrategyCoordinator coordinator = new BackupStrategyCoordinator( fs, consistencyCheckService, internalLogProvider,
                                                                                   progressMonitorFactory, wrapper );
            coordinator.performBackup( context );
        }
    }

    private Set<String> getAllDatabaseNames( Config config, DatabaseNamePattern databaseName, SocketAddress address )
    {
        if ( !databaseName.containsPattern() )
        {
            return Set.of( databaseName.getDatabaseName() );
        }
        else
        {
            try ( BackupSupportingClasses supportingClasses = backupSupportingClassesFactory.createSupportingClasses( config ) )
            {
                final var delegator = supportingClasses.getBackupDelegator();
                try ( Lifespan ignore = new Lifespan( delegator ) )
                {
                    return delegator.getAllDatabaseIds( address )
                                    .stream()
                                    .map( NamedDatabaseId::name )
                                    .collect( Collectors.toSet() );
                }
            }
        }
    }

    private void verify( OnlineBackupContext context ) throws BackupExecutionException
    {
        // user specifies target backup directory and backup procedure creates a sub-directory with the same name as the database
        // verify existence of the directory as specified by the user
        checkDestination( context.getDatabaseBackupDir().getParent() );

        // consistency check report is placed directly into the specified directory, verify its existence
        checkDestination( context.getReportDir() );
    }

    private void checkDestination( Path path ) throws BackupExecutionException
    {
        if ( !fs.isDirectory( path.toFile() ) )
        {
            throw new BackupExecutionException( format( "Directory '%s' does not exist.", path ) );
        }
    }

    public static final class Builder
    {
        private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private LogProvider internalLogProvider = NullLogProvider.getInstance();
        private ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.NONE;
        private Monitors monitors = new Monitors();
        private BackupSupportingClassesFactory supportingClassesFactory;
        private StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        private SystemNanoClock clock;

        private Builder()
        {
        }

        public Builder withFileSystem( FileSystemAbstraction fs )
        {
            this.fs = fs;
            return this;
        }

        public Builder withUserLogProvider( LogProvider userLogProvider )
        {
            this.userLogProvider = userLogProvider;
            return this;
        }

        public Builder withInternalLogProvider( LogProvider internalLogProvider )
        {
            this.internalLogProvider = internalLogProvider;
            return this;
        }

        public Builder withProgressMonitorFactory( ProgressMonitorFactory progressMonitorFactory )
        {
            this.progressMonitorFactory = progressMonitorFactory;
            return this;
        }

        public Builder withMonitors( Monitors monitors )
        {
            this.monitors = monitors;
            return this;
        }

        public Builder withSupportingClassesFactory( BackupSupportingClassesFactory supportingClassesFactory )
        {
            this.supportingClassesFactory = supportingClassesFactory;
            return this;
        }

        public Builder withClock( SystemNanoClock clock )
        {
            this.clock = clock;
            return this;
        }

        public Builder withStorageEngineFactory( StorageEngineFactory storageEngineFactory )
        {
            this.storageEngineFactory = storageEngineFactory;
            return this;
        }

        public OnlineBackupExecutor build()
        {
            if ( supportingClassesFactory == null )
            {
                supportingClassesFactory = new BackupSupportingClassesFactory( storageEngineFactory, fs, internalLogProvider, monitors, clock );
            }

            return new OnlineBackupExecutor( this );
        }
    }
}
