/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.OutputStream;

import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.NullOutputStream;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class OnlineBackupExecutor
{
    private final OutputStream outputStream;
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final Monitors monitors;
    private final BackupSupportingClassesFactory backupSupportingClassesFactory;
    private final ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();

    private OnlineBackupExecutor( Builder builder )
    {
        this.outputStream = builder.outputStream;
        this.fs = builder.fs;
        this.logProvider = builder.logProvider;
        this.progressMonitorFactory = builder.progressMonitorFactory;
        this.monitors = builder.monitors;
        this.backupSupportingClassesFactory = builder.supportingClassesFactory;
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
        try ( BackupSupportingClasses supportingClasses = backupSupportingClassesFactory.createSupportingClasses( context ) )
        {
            LogProvider userLogProvider = setupUserLogProvider();
            PageCache pageCache = supportingClasses.getPageCache();

            StoreFiles storeFiles = new StoreFiles( fs, pageCache );
            BackupCopyService copyService = new BackupCopyService( fs, new FileMoveProvider( fs ) );

            BackupStrategy strategy = new DefaultBackupStrategy( supportingClasses.getBackupDelegator(), logProvider, storeFiles );
            BackupStrategyWrapper wrapper =
                    new BackupStrategyWrapper( strategy, copyService, fs, pageCache, userLogProvider, logProvider, context.getStorageEngineFactory() );

            BackupStrategyCoordinator coordinator = new BackupStrategyCoordinator( fs, consistencyCheckService, logProvider, progressMonitorFactory, wrapper );
            coordinator.performBackup( context );
        }
    }

    private static BackupSupportingClassesFactory createBackupSupportingClassesFactory( OutputStream outputStream, FileSystemAbstraction fs,
            LogProvider logProvider, Monitors monitors, StorageEngineFactory storageEngineFactory )
    {
        BackupModule backupModule = new BackupModule( outputStream, fs, logProvider, monitors, storageEngineFactory );

        BackupSupportingClassesFactoryProvider classesFactoryProvider = BackupSupportingClassesFactoryProvider.getProvidersByPriority()
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "Unable to find a suitable backup supporting classes provider in the classpath" ) );

        return classesFactoryProvider.getFactory( backupModule );
    }

    private LogProvider setupUserLogProvider()
    {
        LogProvider userLogProvider = FormattedLogProvider.toOutputStream( outputStream );

        StoreCopyClientMonitor backupStoreCopyMonitor = new BackupOutputMonitor( userLogProvider );
        monitors.addMonitorListener( backupStoreCopyMonitor );

        return userLogProvider;
    }

    public static final class Builder
    {
        private OutputStream outputStream = NullOutputStream.NULL_OUTPUT_STREAM;
        private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        private LogProvider logProvider = NullLogProvider.getInstance();
        private ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.NONE;
        private Monitors monitors = new Monitors();
        private BackupSupportingClassesFactory supportingClassesFactory;
        private StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine( Service.load( StorageEngineFactory.class ) );

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

        public Builder withStorageEngineFactory( StorageEngineFactory storageEngineFactory )
        {
            this.storageEngineFactory = storageEngineFactory;
            return this;
        }

        public OnlineBackupExecutor build()
        {
            if ( supportingClassesFactory == null )
            {
                supportingClassesFactory = createBackupSupportingClassesFactory( outputStream, fs, logProvider, monitors, storageEngineFactory );
            }

            return new OnlineBackupExecutor( this );
        }
    }
}
