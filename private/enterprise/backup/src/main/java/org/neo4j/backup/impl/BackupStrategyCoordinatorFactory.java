/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.List;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

/*
 * Backup strategy coordinators iterate through backup strategies and make sure at least one of them can perform a valid backup.
 * Handles cases when individual backups aren't  possible.
 */
class BackupStrategyCoordinatorFactory
{
    private final LogProvider logProvider;
    private final ConsistencyCheckService consistencyCheckService;
    private final AddressResolver addressResolver;
    private final OutsideWorld outsideWorld;

    BackupStrategyCoordinatorFactory( BackupModule backupModule )
    {
        this.logProvider = backupModule.getLogProvider();
        this.outsideWorld = backupModule.getOutsideWorld();

        this.consistencyCheckService = new ConsistencyCheckService();
        this.addressResolver = new AddressResolver();
    }

    /**
     * Construct a wrapper of supported backup strategies
     *
     * @param onlineBackupContext the input of the backup tool, such as CLI arguments, config etc.
     * @param backupProtocolService the underlying backup implementation for HA and single node instances
     * @param backupDelegator the backup implementation used for CC backups
     * @param pageCache the page cache used moving files
     * @return strategy coordinator that handles the which backup strategies are tried and establishes if a backup was successful or not
     */
    BackupStrategyCoordinator backupStrategyCoordinator(
            OnlineBackupContext onlineBackupContext, BackupProtocolService backupProtocolService,
            BackupDelegator backupDelegator, PageCache pageCache )
    {
        FileSystemAbstraction fs = outsideWorld.fileSystem();
        BackupCopyService copyService = new BackupCopyService( fs, new FileMoveProvider( fs ) );
        ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.textual( outsideWorld.errorStream() );
        BackupRecoveryService recoveryService = new BackupRecoveryService();
        long timeout = onlineBackupContext.getRequiredArguments().getTimeout();
        Config config = onlineBackupContext.getConfig();

        StoreFiles storeFiles = new StoreFiles( fs, pageCache );
        BackupStrategy ccStrategy = new CausalClusteringBackupStrategy( backupDelegator, addressResolver, logProvider, storeFiles );
        BackupStrategy haStrategy = new HaBackupStrategy( backupProtocolService, addressResolver, logProvider, timeout );

        BackupStrategyWrapper ccStrategyWrapper = wrap( ccStrategy, copyService, pageCache, config, recoveryService );
        BackupStrategyWrapper haStrategyWrapper = wrap( haStrategy, copyService, pageCache, config, recoveryService );
        StrategyResolverService strategyResolverService = new StrategyResolverService( haStrategyWrapper, ccStrategyWrapper );
        List<BackupStrategyWrapper> strategies =
                strategyResolverService.getStrategies( onlineBackupContext.getRequiredArguments().getSelectedBackupProtocol() );

        return new BackupStrategyCoordinator( consistencyCheckService, outsideWorld, logProvider, progressMonitorFactory, strategies );
    }

    private BackupStrategyWrapper wrap( BackupStrategy strategy, BackupCopyService copyService, PageCache pageCache,
                                        Config config, BackupRecoveryService recoveryService )
    {
        return new BackupStrategyWrapper( strategy, copyService, pageCache, config, recoveryService, logProvider ) ;
    }
}
