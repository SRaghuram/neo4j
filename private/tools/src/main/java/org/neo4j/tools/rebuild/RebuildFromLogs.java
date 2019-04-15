/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.rebuild;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.InconsistentStoreException;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.cursor.IOCursor;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

/**
 * Tool to rebuild store based on available transaction logs.
 */
class RebuildFromLogs
{
    private static final String UP_TO_TX_ID = "tx";

    private final FileSystemAbstraction fs;
    private static DatabaseManagementService managementService;

    RebuildFromLogs( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public static void main( String[] args ) throws Exception, InconsistentStoreException
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = Args.parse( args );
        @SuppressWarnings( "boxing" )
        long txId = params.getNumber( UP_TO_TX_ID, BASE_TX_ID ).longValue();
        List<String> orphans = params.orphans();
        args = orphans.toArray( new String[0] );
        if ( args.length != 2 )
        {
            printUsage( "Exactly two positional arguments expected: " +
                        "<source dir with logs> <target dir for graphdb>, got " + args.length );
            System.exit( -1 );
            return;
        }
        File source = new File( args[0] );
        File target = new File( args[1] );
        if ( !source.isDirectory() )
        {
            printUsage( source + " is not a directory" );
            System.exit( -1 );
            return;
        }
        if ( target.exists() )
        {
            if ( target.isDirectory() )
            {
                if ( directoryContainsDb( target.toPath() ) )
                {
                    printUsage( "target graph database already exists" );
                    System.exit( -1 );
                    return;
                }
                System.err.println( "WARNING: the directory " + target + " already exists" );
            }
            else
            {
                printUsage( target + " is a file" );
                System.exit( -1 );
                return;
            }
        }

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            new RebuildFromLogs( fileSystem ).rebuild( DatabaseLayout.of( source ), DatabaseLayout.of( target ), txId );
        }
    }

    private static boolean directoryContainsDb( Path path )
    {
        return Files.exists( DatabaseLayout.of( path.toFile() ).metadataStore().toPath() );
    }

    public void rebuild( DatabaseLayout sourceDatabaseLayout, DatabaseLayout targetLayout, long txId ) throws Exception, InconsistentStoreException
    {
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, createInitialisedScheduler() ) )
        {
            File transactionLogsDirectory = sourceDatabaseLayout.getTransactionLogsDirectory();
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( transactionLogsDirectory, fs ).build();
            long highestVersion = logFiles.getHighestLogVersion();
            if ( highestVersion < 0 )
            {
                printUsage( "Inconsistent number of log files found in " + transactionLogsDirectory );
                return;
            }

            long lastTxId;
            try ( TransactionApplier applier = new TransactionApplier( fs, targetLayout, pageCache ) )
            {
                lastTxId = applier.applyTransactionsFrom( transactionLogsDirectory, txId );
            }

            // set last tx id in neostore otherwise the db is not usable
            MetaDataStore.setRecord( pageCache, targetLayout.metadataStore(),
                    MetaDataStore.Position.LAST_TRANSACTION_ID, lastTxId );

            checkConsistency( targetLayout, pageCache );
        }
    }

    void checkConsistency( DatabaseLayout layout, PageCache pageCache ) throws Exception, InconsistentStoreException
    {
        try ( ConsistencyChecker checker = new ConsistencyChecker( layout, pageCache ) )
        {
            checker.checkConsistency();
        }
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines )
        {
            System.err.println( line );
        }
        System.err.println( Args.jarUsage( RebuildFromLogs.class,
                "[-full] <source dir with logs> <target dir for graphdb>" ) );
        System.err.println( "WHERE:   <source dir>  is the path for where transactions to rebuild from are stored" );
        System.err.println( "         <target dir>  is the path for where to create the new graph database" );
        System.err.println( "         -tx       --  to rebuild the store up to a given transaction" );
    }

    private static class TransactionApplier implements AutoCloseable
    {
        private final GraphDatabaseAPI graphdb;
        private final FileSystemAbstraction fs;
        private final TransactionCommitProcess commitProcess;

        TransactionApplier( FileSystemAbstraction fs, DatabaseLayout layout, PageCache pageCache )
        {
            this.fs = fs;
            this.graphdb = startTemporaryDb( layout, pageCache );
            this.commitProcess = graphdb.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        }

        long applyTransactionsFrom( File sourceDir, long upToTxId ) throws Exception
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( sourceDir, fs ).build();
            int startVersion = 0;
            ReaderLogVersionBridge versionBridge = new ReaderLogVersionBridge( logFiles );
            PhysicalLogVersionedStoreChannel startingChannel = logFiles.openForVersion( startVersion );
            ReadableLogChannel channel = new ReadAheadLogChannel( startingChannel, versionBridge );
            long txId = BASE_TX_ID;
            TransactionQueue queue = new TransactionQueue( 10_000,
                    ( tx, last ) -> commitProcess.commit( tx, NULL, EXTERNAL ) );
            LogEntryReader<ReadableClosablePositionAwareChannel> entryReader = new VersionAwareLogEntryReader<>();
            try ( IOCursor<CommittedTransactionRepresentation> cursor =
                    new PhysicalTransactionCursor<>( channel, entryReader ) )
            {
                while ( cursor.next() )
                {
                    txId = cursor.get().getCommitEntry().getTxId();
                    TransactionRepresentation transaction = cursor.get().getTransactionRepresentation();
                    queue.queue( new TransactionToApply( transaction, txId ) );
                    if ( upToTxId != BASE_TX_ID && upToTxId == txId )
                    {
                        break;
                    }
                }
            }
            queue.empty();
            return txId;
        }

        @Override
        public void close()
        {
            managementService.shutdown();
        }
    }

    private static class ConsistencyChecker implements AutoCloseable
    {
        private final GraphDatabaseAPI graphdb;
        private final LabelScanStore labelScanStore;
        private final Config tuningConfiguration = Config.defaults();
        private final IndexProviderMap indexes;

        ConsistencyChecker( DatabaseLayout layout, PageCache pageCache )
        {
            this.graphdb = startTemporaryDb( layout, pageCache );
            DependencyResolver resolver = graphdb.getDependencyResolver();
            this.labelScanStore = resolver.resolveDependency( LabelScanStore.class );
            this.indexes = resolver.resolveDependency( IndexProviderMap.class );
        }

        private void checkConsistency() throws ConsistencyCheckIncompleteException, InconsistentStoreException
        {
            RecordStorageEngine storageEngine = graphdb.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
            StoreAccess nativeStores = new StoreAccess( storageEngine.testAccessNeoStores() ).initialize();
            DirectStoreAccess stores = new DirectStoreAccess( nativeStores, labelScanStore, indexes, storageEngine.testAccessCountsStore() );
            FullCheck fullCheck = new FullCheck( tuningConfiguration, ProgressMonitorFactory.textual( System.err ),
                    Statistics.NONE, ConsistencyCheckService.defaultConsistencyCheckThreadsNumber() );

            ConsistencySummaryStatistics summaryStatistics =
                    fullCheck.execute( stores, FormattedLog.toOutputStream( System.err ) );
            if ( !summaryStatistics.isConsistent() )
            {
                throw new InconsistentStoreException( summaryStatistics );
            }

        }

        @Override
        public void close()
        {
            managementService.shutdown();
        }
    }

    private static GraphDatabaseAPI startTemporaryDb( DatabaseLayout databaseLayout, PageCache pageCache )
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( new ExternallyManagedPageCache( pageCache ) );
        managementService = new CommercialGraphDatabaseFactory()
                .setExternalDependencies( dependencies )
                .newEmbeddedDatabaseBuilder( databaseLayout.getStoreLayout().storeDirectory() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.default_database, databaseLayout.getDatabaseName() )
                .newDatabaseManagementService();
        return (GraphDatabaseAPI) managementService.database( databaseLayout.getDatabaseName() );
    }
}
