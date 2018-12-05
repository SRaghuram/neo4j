/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.LogProvider;

/*
 * Backup strategy coordinators iterate through backup strategies and make sure at least one of them can perform a valid backup.
 * Handles cases when individual backups aren't  possible.
 */
class BackupStrategyCoordinatorFactory
{
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final ConsistencyCheckService consistencyCheckService;
    private final AddressResolver addressResolver;

    BackupStrategyCoordinatorFactory( BackupModule backupModule )
    {
        this.fs = backupModule.getFileSystem();
        this.logProvider = backupModule.getLogProvider();

        this.consistencyCheckService = new ConsistencyCheckService();
        this.addressResolver = new AddressResolver();
    }

    /**
     * Construct a wrapper of supported backup strategies
     *
     * @param onlineBackupContext the input of the backup tool, such as CLI arguments, config etc.
     * @param backupDelegator the backup implementation used for CC backups
     * @param pageCache the page cache used moving files
     * @return strategy coordinator that handles the which backup strategies are tried and establishes if a backup was successful or not
     */
    BackupStrategyCoordinator backupStrategyCoordinator( OnlineBackupContext onlineBackupContext, BackupDelegator backupDelegator,
            PageCache pageCache, ProgressMonitorFactory progressMonitorFactory )
    {
        BackupCopyService copyService = new BackupCopyService( fs, new FileMoveProvider( fs ) );
        long timeout = onlineBackupContext.getRequiredArguments().getTimeout(); // todo: this should be used!

        StoreFiles storeFiles = new StoreFiles( fs, pageCache );
        BackupStrategy backupStrategy = new DefaultBackupStrategy( backupDelegator, addressResolver, logProvider, storeFiles );

        BackupStrategyWrapper strategyWrapper = new BackupStrategyWrapper( backupStrategy, copyService, fs, pageCache, logProvider );

        return new BackupStrategyCoordinator( fs, consistencyCheckService, logProvider, progressMonitorFactory, strategyWrapper );
    }
}
