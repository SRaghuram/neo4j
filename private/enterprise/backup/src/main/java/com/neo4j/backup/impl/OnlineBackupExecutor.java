/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.backup.impl.remote.BackupClient;
import com.neo4j.backup.impl.tools.BackupConsistencyChecker;
import com.neo4j.backup.impl.tools.BackupRecoveryService;
import com.neo4j.backup.impl.tools.CheckConsistency;
import com.neo4j.backup.impl.tools.ConsistencyCheckExecutionException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

public final class OnlineBackupExecutor
{
    private final FileSystemAbstraction fs;
    private final LogProvider userLogProvider;
    private final LogProvider internalLogProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final Monitors monitors;
    private final SystemNanoClock clock;
    private final StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();

    private OnlineBackupExecutor( Builder builder )
    {
        this.fs = builder.fs;
        this.userLogProvider = builder.userLogProvider;
        this.internalLogProvider = builder.internalLogProvider;
        this.progressMonitorFactory = builder.progressMonitorFactory;
        this.monitors = builder.monitors;
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

    public void executeBackups( OnlineBackupContext context ) throws Exception
    {
        verify( context );
        final var userLog = userLogProvider.getLog( getClass() );
        final var internalLog = internalLogProvider.getLog( getClass() );

        var pageCacheTracer = PageCacheTracer.NULL;
        var consistencyChecker = consistencyChecker( context );

        var backupResults = new ArrayList<BackupResult>();
        var dbNamePattern = context.getDatabaseNamePattern();
        var config = context.getConfig();

        try ( BackupsLifecycle lifecycle = BackupsLifecycle.startLifecycle( storageEngineFactory, fs, internalLogProvider, clock, config, pageCacheTracer ) )
        {
            var backupClient = backupClient( context, pageCacheTracer, lifecycle, lifecycle.getJobScheduler() );
            var recoveryService = recoveryService( context, lifecycle );

            var dbNames = getAllDatabaseNames( backupClient, context );
            if ( dbNames.isEmpty() )
            {
                throw new BackupExecutionException(
                        String.format( "%s doesn't match any database on the remote server", dbNamePattern ) );
            }

            for ( String databaseName : dbNames )
            {
                try
                {
                    BackupOutputMonitor backupMonitor = new BackupOutputMonitor( userLogProvider, clock );

                    var databaseBackup = new OnlineDatabaseBackup( fs, internalLogProvider, userLog );
                    databaseBackup.backup( context, pageCacheTracer, consistencyChecker, lifecycle, recoveryService, backupClient, databaseName,
                                           backupMonitor );
                    backupResults.add( new BackupResult( databaseName ) );
                }
                catch ( Exception e )
                {
                    internalLog.error( "Error in database " + databaseName, e );
                    backupResults.add( new BackupResult( databaseName, e ) );
                }
            }
        }

        final var inputContainsPattern = dbNamePattern.containsPattern();
        if ( !inputContainsPattern )
        {
            //if there is no pattern in the name, there should be only one DB that matches the name on the server side
            final var exception = backupResults.isEmpty() ? null : backupResults.get( 0 ).exception;
            if ( exception != null )
            {
                throw exception;
            }
        }
        else
        {
            backupResults.forEach( r -> printBackupResult( userLog, r ) );
            final var oneFailedBackup = backupResults.stream().anyMatch( r -> !r.success() );
            if ( oneFailedBackup )
            {
                throw new BackupExecutionException( "Not all databases are backed up" );
            }
        }
    }

    private Set<String> getAllDatabaseNames( BackupClient backupClient, OnlineBackupContext context ) throws Exception
    {
        DatabaseNamePattern pattern = context.getDatabaseNamePattern();
        if ( !pattern.containsPattern() )
        {
            return Set.of( pattern.getDatabaseName() );
        }
        else
        {
            return backupClient.getAllDatabaseNames( context.getAddress() ).stream()
                               .map( NamedDatabaseId::name )
                               .filter( pattern::matches )
                               .collect( Collectors.toSet() );
        }
    }

    private BackupRecoveryService recoveryService( OnlineBackupContext context, BackupsLifecycle lifecycle )
    {
        return new BackupRecoveryService( fs, lifecycle.getPageCache(), context.getConfig(), context.getMemoryTracker() );
    }

    private BackupClient backupClient( OnlineBackupContext context, PageCacheTracer pageCacheTracer, BackupsLifecycle lifecycle,
                                       JobScheduler jobScheduler )
    {
        return new BackupClient( internalLogProvider, lifecycle.getCatchupClientFactory(), fs, lifecycle.getPageCache(), pageCacheTracer, monitors,
                                 context.getConfig(), storageEngineFactory, clock, jobScheduler );
    }

    private BackupConsistencyChecker consistencyChecker( OnlineBackupContext context )
    {
        if ( context.consistencyCheckEnabled() )
        {
            var config = context.getConfig();
            var consistencyFlags = context.getConsistencyFlags();
            var reportDir = context.getReportDir();
            return new CheckConsistency( config, consistencyFlags, reportDir, progressMonitorFactory, fs, internalLogProvider );
        }
        else
        {
            return BackupConsistencyChecker.NO_OP;
        }
    }

    private void verify( OnlineBackupContext context ) throws BackupExecutionException
    {
        // user specifies target backup directory and backup procedure creates a sub-directory with the same name as the database
        // verify existence of the directory as specified by the user
        var backupDirParent = context.getBackupDir();
        checkDestination( backupDirParent );

        // consistency check report is placed directly into the specified directory, verify its existence
        checkDestination( context.getReportDir() );
    }

    private void checkDestination( Path path ) throws BackupExecutionException
    {
        if ( !fs.isDirectory( path ) )
        {
            throw new BackupExecutionException( format( "Directory '%s' does not exist.", path ) );
        }
    }

    private void printBackupResult( Log log, BackupResult backupResult )
    {
        var reason = "";
        if ( backupResult.exception != null )
        {
            if ( backupResult.exception instanceof ConsistencyCheckExecutionException )
            {
                reason = backupResult.exception.getMessage();
            }
            else
            {
                reason = getRootCause( backupResult.exception ).getMessage();
            }
        }
        final var status = backupResult.success() ? "successful" : "failed";
        final var databaseName = backupResult.databaseName;
        log.info( "databaseName=%s, backupStatus=%s, reason=%s", databaseName, status, reason );
    }

    public static final class Builder
    {
        private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private LogProvider internalLogProvider = NullLogProvider.getInstance();
        private ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.NONE;
        private Monitors monitors = new Monitors();
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

        public Builder withClock( SystemNanoClock clock )
        {
            this.clock = clock;
            return this;
        }

        public OnlineBackupExecutor build()
        {
            return new OnlineBackupExecutor( this );
        }
    }

    private static final class BackupResult
    {
        public final String databaseName;
        public final Exception exception;

        private BackupResult( String databaseName, Exception exception )
        {
            this.databaseName = databaseName;
            this.exception = exception;
        }

        private BackupResult( String databaseName )
        {
            this.databaseName = databaseName;
            this.exception = null;
        }

        public boolean success()
        {
            return exception == null;
        }
    }
}
