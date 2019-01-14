/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.common.ProgressReporter;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.index.internal.gbptree.GroupingRecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DefaultForceOperation;
import org.neo4j.kernel.extension.DatabaseKernelExtensions;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.DefaultIdController;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.RecoveryThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.monitoring.LogProgressReporter;
import org.neo4j.kernel.internal.DatabaseEventHandlers;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.neo4j.helpers.collection.Iterables.stream;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.constraints.ConstraintSemantics.getConstraintSemantics;
import static org.neo4j.kernel.impl.core.TokenHolder.TYPE_LABEL;
import static org.neo4j.kernel.impl.core.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.kernel.impl.core.TokenHolder.TYPE_RELATIONSHIP_TYPE;

/**
 * Utility class to perform store recovery or check is recovery is required.
 * Recovery is required and can/will be performed on database that have at least one transaction in transaction log after last available checkpoint.
 * During recovery all recorded changes from transaction logs will be replayed and in the end checkpoint will be performed.
 * Please note that recovery will not gonna wait for all affected indexes populations to finish.
 */
public final class Recovery
{
    private Recovery()
    {
    }

    /**
     * Return helper that can be used to check if some database described by {@link DatabaseLayout} requires recovery.
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param config custom configuration
     * @return helper recovery checker
     */
    public static RecoveryRequiredChecker recoveryRequiredChecker( FileSystemAbstraction fs, PageCache pageCache, Config config )
    {
        return new RecoveryRequiredChecker( fs, pageCache, config );
    }

    /**
     * Check if recovery is required for a store described by provided layout.
     * Custom root location for transaction logs can be provided using {@link GraphDatabaseSettings#transaction_logs_root_path} config setting value.
     * @param databaseLayout layout of database to check for recovery
     * @param config custom configuration
     * @return true if recovery is required, false otherwise.
     * @throws Exception
     */
    public static boolean isRecoveryRequired( DatabaseLayout databaseLayout, Config config ) throws Exception
    {
        requireNonNull( databaseLayout );
        requireNonNull( config );
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction() )
        {
            return isRecoveryRequired( fs, databaseLayout, config );
        }
    }

    /**
     * Check if recovery is required for a store described by provided layout.
     * Custom root location for transaction logs can be provided using {@link GraphDatabaseSettings#transaction_logs_root_path} config setting value.
     * @param fs database filesystem
     * @param databaseLayout layout of database to check for recovery
     * @param config custom configuration
     * @return true if recovery is required, false otherwise.
     * @throws Exception
     */
    public static boolean isRecoveryRequired( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config ) throws Exception
    {
        requireNonNull( databaseLayout );
        requireNonNull( config );
        requireNonNull( fs );
        try ( JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = getPageCache( config, fs, jobScheduler ) )
        {
            return isRecoveryRequired( fs, pageCache, databaseLayout, config, Optional.empty() );
        }
    }

    /**
     * Performs recovery of database described by provided layout.
     * Transaction logs should be located in their default location.
     * If recovery is not required nothing will be done to the the database or logs.
     * @param databaseLayout database to recover layout.
     * @throws Exception
     */
    public static void performRecovery( DatabaseLayout databaseLayout ) throws Exception
    {
        requireNonNull( databaseLayout );
        Config config = defaults();
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = getPageCache( config, fs, jobScheduler ) )
        {
            performRecovery( fs, pageCache, config, databaseLayout );
        }
    }

    /**
     * Performs recovery of database described by provided layout.
     * <b>Transaction logs should be located in their default location and any provided custom location is ignored.</b>
     * If recovery is not required nothing will be done to the the database or logs.
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param config custom configuration
     * @param databaseLayout database to recover layout.
     * @throws Exception
     */
    public static void performRecovery( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout ) throws IOException
    {
        requireNonNull( fs );
        requireNonNull( pageCache );
        requireNonNull( config );
        requireNonNull( databaseLayout );
        Map<String,String> configRaw = config.getRaw();
        //remove any custom logical logs location
        configRaw.remove( GraphDatabaseSettings.transaction_logs_root_path.name() );
        Config recoveryConfig = defaults( configRaw );
        performRecovery( fs, pageCache, recoveryConfig, databaseLayout, NullLogProvider.getInstance(), new Monitors(), loadExtensions(), Optional.empty() );
    }

    /**
     * Performs recovery of database described by provided layout.
     * @param fs database filesystem
     * @param pageCache page cache used to perform database recovery.
     * @param config custom configuration
     * @param databaseLayout database to recover layout.
     * @param logProvider log provider
     * @param globalMonitors global server monitors
     * @param extensionFactories extension factories for extensions that should participate in recovery
     * @param providedLogScanner log scanner
     * @throws IOException
     */
    public static void performRecovery( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout, LogProvider logProvider,
            Monitors globalMonitors, Iterable<KernelExtensionFactory<?>> extensionFactories, Optional<LogTailScanner> providedLogScanner )
            throws IOException
    {
        Log recoveryLog = logProvider.getLog( Recovery.class );
        if ( !isRecoveryRequired( fs, pageCache, databaseLayout, config, providedLogScanner ) )
        {
            return;
        }
        LifeSupport recoveryLife = new LifeSupport();
        Monitors monitors = new Monitors( globalMonitors );
        SimpleLogService logService = new SimpleLogService( logProvider );
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();

        DatabaseSchemaState schemaState = new DatabaseSchemaState( logProvider );
        JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();

        DatabasePanicEventGenerator panicEventGenerator = new DatabasePanicEventGenerator( new DatabaseEventHandlers( recoveryLog ) );
        DatabaseHealth databaseHealth = new DatabaseHealth( panicEventGenerator, recoveryLog );

        TokenHolders tokenHolders = new TokenHolders( new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TYPE_RELATIONSHIP_TYPE ) );
        TokenNameLookup tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders );

        RecoveryCleanupWorkCollector recoveryCleanupCollector = new GroupingRecoveryCleanupWorkCollector( scheduler );
        DatabaseKernelExtensions extensions = instantiateRecoveryExtensions( databaseLayout, fs, config, logService, pageCache, scheduler,
                recoveryCleanupCollector, DatabaseInfo.TOOL, monitors, tokenHolders, recoveryCleanupCollector, extensionFactories );
        DefaultIndexProviderMap indexProviderMap = new DefaultIndexProviderMap( extensions, config );

        RecordStorageEngine storageEngine =
                new RecordStorageEngine( databaseLayout, config, pageCache, fs, logProvider, tokenHolders, schemaState,
                        getConstraintSemantics(), LockService.NO_LOCK_SERVICE,
                        databaseHealth, new DefaultIdGeneratorFactory( fs ), new DefaultIdController(), EmptyVersionContextSupplier.EMPTY );

        // Label index
        NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE, storageEngine::newReader );
        LabelScanStore labelScanStore = Database.buildLabelIndex( recoveryCleanupCollector, storageEngine, neoStoreIndexStoreView, monitors,
                logProvider, pageCache, databaseLayout, fs, false );

        // Schema indexes
        DynamicIndexStoreView indexStoreView =
                new DynamicIndexStoreView( neoStoreIndexStoreView, labelScanStore, LockService.NO_LOCK_SERVICE, storageEngine::newReader, logProvider );
        IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout, recoveryCleanupCollector );
        IndexingService indexingService = Database.buildIndexingService( storageEngine, schemaState, indexStoreView, indexStatisticsStore,
                config, scheduler, indexProviderMap, tokenNameLookup, logProvider, logProvider, monitors.newMonitor( IndexingService.Monitor.class ) );

        Dependencies recoveryDependencies = new Dependencies();
        storageEngine.satisfyDependencies( recoveryDependencies );

        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( logEntryReader )
                .withConfig( config )
                .withDependencies( recoveryDependencies )
                .build();

        TransactionIdStore transactionIdStore = recoveryDependencies.resolveDependency( TransactionIdStore.class );
        LogVersionRepository logVersionRepository = recoveryDependencies.resolveDependency( LogVersionRepository.class );

        Boolean failOnCorruptedLogFiles = config.get( GraphDatabaseSettings.fail_on_corrupted_log_files );
        LogTailScanner logTailScanner = providedLogScanner.orElseGet( () -> new LogTailScanner( logFiles, logEntryReader, monitors, failOnCorruptedLogFiles ) );
        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        PhysicalLogicalTransactionStore transactionStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, logEntryReader, monitors,
                failOnCorruptedLogFiles );
        BatchingTransactionAppender transactionAppender = new BatchingTransactionAppender( logFiles, LogRotation.NO_ROTATION, metadataCache,
                transactionIdStore, databaseHealth );

        TransactionLogsRecovery transactionLogsRecovery =
                transactionLogRecovery( fs, transactionIdStore, logTailScanner, monitors.newMonitor( RecoveryMonitor.class ),
                        monitors.newMonitor( RecoveryStartInformationProvider.Monitor.class ), logFiles, storageEngine, transactionStore, logVersionRepository,
                        databaseLayout, failOnCorruptedLogFiles, recoveryLog );

        CheckPointerImpl.ForceOperation forceOperation = new DefaultForceOperation( indexingService, labelScanStore, storageEngine );
        CheckPointerImpl checkPointer =
                new CheckPointerImpl( transactionIdStore, RecoveryThreshold.INSTANCE, forceOperation, LogPruning.NO_PRUNING, transactionAppender,
                        databaseHealth, logProvider, CheckPointTracer.NULL, IOLimiter.UNLIMITED, new StoreCopyCheckPointMutex(),
                        monitors.newMonitor( CheckPointerMonitor.class ) );

        recoveryLife.add( scheduler );
        recoveryLife.add( recoveryCleanupCollector );
        recoveryLife.add( extensions );
        recoveryLife.add( indexProviderMap );
        recoveryLife.add( storageEngine );
        recoveryLife.add( labelScanStore );
        recoveryLife.add( indexingService );
        recoveryLife.add( transactionLogsRecovery );
        recoveryLife.add( logFiles );
        recoveryLife.add( transactionAppender );
        recoveryLife.add( checkPointer );
        recoveryLife.start();

        if ( databaseHealth.isHealthy() )
        {
            checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Recovery completed." ) );
        }
        recoveryLife.shutdown();
    }

    private static TransactionLogsRecovery transactionLogRecovery( FileSystemAbstraction fileSystemAbstraction, TransactionIdStore transactionIdStore,
            LogTailScanner tailScanner, RecoveryMonitor recoveryMonitor, RecoveryStartInformationProvider.Monitor positionMonitor, LogFiles logFiles,
            StorageEngine storageEngine, LogicalTransactionStore logicalTransactionStore, LogVersionRepository logVersionRepository,
            DatabaseLayout databaseLayout, boolean failOnCorruptedLogFiles, Log log )
    {
        RecoveryService recoveryService =
                new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore, logicalTransactionStore, logVersionRepository, positionMonitor );
        CorruptedLogsTruncator logsTruncator = new CorruptedLogsTruncator( databaseLayout.databaseDirectory(), logFiles, fileSystemAbstraction );
        ProgressReporter progressReporter = new LogProgressReporter( log );
        return new TransactionLogsRecovery( recoveryService, logsTruncator, recoveryMonitor, progressReporter, failOnCorruptedLogFiles );
    }

    private static Iterable<KernelExtensionFactory<?>> loadExtensions()
    {
        return Iterables.cast( Service.load( KernelExtensionFactory.class ) );
    }

    private static DatabaseKernelExtensions instantiateRecoveryExtensions( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, Config config,
            LogService logService, PageCache pageCache, JobScheduler jobScheduler, RecoveryCleanupWorkCollector recoveryCollector, DatabaseInfo databaseInfo,
            Monitors monitors, TokenHolders tokenHolders, RecoveryCleanupWorkCollector recoveryCleanupCollector,
            Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        List<KernelExtensionFactory<?>> recoveryExtensions = stream( kernelExtensions )
                        .filter( extension -> extension.getClass().isAnnotationPresent( RecoveryExtension.class ) )
                        .collect( toList() );

        Dependencies deps = new Dependencies();
        NonListenableMonitors nonListenableMonitors = new NonListenableMonitors( monitors );
        deps.satisfyDependencies( fileSystem, config, logService, pageCache, recoveryCollector, nonListenableMonitors, jobScheduler,
                tokenHolders, recoveryCleanupCollector );
        DatabaseExtensionContext extensionContext = new DatabaseExtensionContext( databaseLayout, databaseInfo, deps );
        return new DatabaseKernelExtensions( extensionContext, recoveryExtensions, deps, KernelExtensionFailureStrategies.fail() );
    }

    private static boolean isRecoveryRequired( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout, Config config,
            Optional<LogTailScanner> logTailScanner ) throws IOException
    {
        RecoveryRequiredChecker requiredChecker = recoveryRequiredChecker( fs, pageCache, config );
        return logTailScanner.isPresent() ? requiredChecker.isRecoveryRequiredAt( databaseLayout, logTailScanner.get() )
                                          : requiredChecker.isRecoveryRequiredAt( databaseLayout );
    }

    private static PageCache getPageCache( Config config, FileSystemAbstraction fs, JobScheduler jobScheduler )
    {
        ConfiguringPageCacheFactory pageCacheFactory =
                new ConfiguringPageCacheFactory( fs, config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance(),
                        EmptyVersionContextSupplier.EMPTY, jobScheduler );
        return pageCacheFactory.getOrCreatePageCache();
    }

    static void throwUnableToCleanRecover( Throwable t )
    {
        throw new RuntimeException(
                "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify '" +
                        GraphDatabaseSettings.fail_on_corrupted_log_files.name() + "=false'. This will try to recover as much " +
                        "as possible and then truncate the corrupt part of the transaction log. Doing this means your database " +
                        "integrity might be compromised, please consider restoring from a consistent backup instead.", t );
    }

    // We need to create monitors that do not allow listener registration here since at this point another version of extensions already stared by owning
    // database life and if we will allow registration of listeners here we will end-up having same event captured by multiple listeners resulting in
    // for example duplicated logging records in user facing logs
    private static class NonListenableMonitors extends Monitors
    {
        NonListenableMonitors( Monitors monitors )
        {
            super( monitors );
        }

        @Override
        public void addMonitorListener( Object monitorListener, String... tags )
        {
        }
    }
}
