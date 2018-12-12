/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.OutputStream;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.NullOutputStream;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class OnlineBackupExecutor
{
    private final OutputStream outputStream;
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final Monitors monitors;

    private final AddressResolver addressResolver = new AddressResolver();
    private final ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();

    private OnlineBackupExecutor( Builder builder )
    {
        this.outputStream = builder.outputStream;
        this.fs = builder.fs;
        this.logProvider = builder.logProvider;
        this.progressMonitorFactory = builder.progressMonitorFactory;
        this.monitors = new Monitors();
    }

    public static OnlineBackupExecutor buildDefault()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public void executeBackup( OnlineBackupContext context ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        try ( BackupSupportingClasses supportingClasses = createBackupSupportingClasses( context ) )
        {
            PageCache pageCache = supportingClasses.getPageCache();

            StoreFiles storeFiles = new StoreFiles( fs, pageCache );
            BackupCopyService copyService = new BackupCopyService( fs, new FileMoveProvider( fs ) );

            BackupStrategy strategy = new DefaultBackupStrategy( supportingClasses.getBackupDelegator(), addressResolver, logProvider, storeFiles );
            BackupStrategyWrapper wrapper = new BackupStrategyWrapper( strategy, copyService, fs, pageCache, logProvider );

            BackupStrategyCoordinator coordinator = new BackupStrategyCoordinator( fs, consistencyCheckService, logProvider, progressMonitorFactory, wrapper );
            coordinator.performBackup( context );
        }
    }

    private BackupSupportingClasses createBackupSupportingClasses( OnlineBackupContext context )
    {
        BackupModule backupModule = new BackupModule( outputStream, fs, logProvider, monitors );

        BackupSupportingClassesFactoryProvider classesFactoryProvider = BackupSupportingClassesFactoryProvider.getProvidersByPriority()
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "Unable to find a suitable backup supporting classes provider in the classpath" ) );

        BackupSupportingClassesFactory factory = classesFactoryProvider.getFactory( backupModule );

        return factory.createSupportingClasses( context );
    }

    public static final class Builder
    {
        private OutputStream outputStream = NullOutputStream.NULL_OUTPUT_STREAM;
        private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        private LogProvider logProvider = NullLogProvider.getInstance();
        private ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.NONE;

        private Builder()
        {
        }

        public Builder withOutputStream( OutputStream outputStream )
        {
            this.outputStream = outputStream;
            return this;
        }

        public Builder withFileSystem( FileSystemAbstraction fs )
        {
            this.fs = fs;
            return this;
        }

        public Builder withLogProvider( LogProvider logProvider )
        {
            this.logProvider = logProvider;
            return this;
        }

        public Builder withProgressMonitorFactory( ProgressMonitorFactory progressMonitorFactory )
        {
            this.progressMonitorFactory = progressMonitorFactory;
            return this;
        }

        public OnlineBackupExecutor build()
        {
            return new OnlineBackupExecutor( this );
        }
    }
}
