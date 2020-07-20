/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.ManagedStore;
import com.neo4j.bench.micro.data.StringGenerator;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.SplittableRandom;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryInlinedCheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.bench.micro.data.DataGenerator.GraphWriter.TRANSACTIONAL;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class DatabaseRecovery extends AbstractCoreBenchmark
{
    @ParamValues(
            allowed = {"10000", "100000", "1000000"},
            base = {"10000", "100000"} )
    @Param( {} )
    public int transactionsToRecover;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String storeFormat;

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Neo4jConfig neo4jConfig = Neo4jConfigBuilder
                .empty()
                .withSetting( record_format, storeFormat )
                .withSetting( GraphDatabaseSettings.check_point_interval_tx, String.valueOf( Integer.MAX_VALUE ) )
                .build();
        return new DataGeneratorConfigBuilder()
                .withGraphWriter( TRANSACTIONAL )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( false )
                .build();
    }

    private Long expectedNumberOfRecoveredTransactions;

    @Override
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
        GraphDatabaseService db = managedStore.db();

        GraphDatabaseAPI graphDatabaseService = (GraphDatabaseAPI) db;
        TransactionIdStore transactionIdStore = graphDatabaseService
                .getDependencyResolver()
                .resolveDependency( TransactionIdStore.class );
        long transactionIdBefore = transactionIdStore.getLastCommittedTransactionId();

        ValueGeneratorFun<String> stringGenerator = StringGenerator.randShortAlphaNumerical().create();
        SplittableRandom rng = RNGState.newRandom( 42 );
        for ( int i = 0; i < transactionsToRecover; i++ )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                Node nodeA = transaction.createNode();
                Node nodeB = transaction.createNode();
                String type = String.valueOf( i % 10 );
                nodeA.setProperty( type, stringGenerator.next( rng ) );
                nodeA.createRelationshipTo( nodeB, RelationshipType.withName( type ) );
                transaction.commit();
            }
        }

        long transactionIdAfter = transactionIdStore.getLastCommittedTransactionId();
        expectedNumberOfRecoveredTransactions = transactionIdAfter - transactionIdBefore;
    }

    @Setup( Level.Iteration )
    public void truncateCheckpointFromLogs() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) managedStore.db();
        DatabaseLayout databaseLayout = db.databaseLayout();
        StorageEngineFactory storageEngineFactory = db.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        ManagedStore.getManagementService().shutdown();
        removeLastCheckpointsRecordFromLastLogFile( databaseLayout, storageEngineFactory );
        if ( !isRecoveryRequired( databaseLayout, Config.defaults(), INSTANCE ) )
        {
            throw new IllegalStateException( "Store should require recovery." );
        }
    }

    @Benchmark
    @BenchmarkMode( Mode.SingleShotTime )
    public void recoverDatabase()
    {
        managedStore.startDb();
    }

    @Override
    public String description()
    {
        return "Test performance of recovery with different number of transactions that needs to be recovered.";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    private static void removeLastCheckpointsRecordFromLastLogFile( DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory ) throws Exception
    {
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem ).build();

        LogFile logFile = logFiles.getLogFile();
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        ReadableLogChannel reader = logFile.getReader( logFile.extractHeader( logFile.getHighestLogVersion() ).getStartPosition() );
        LogEntry logEntry;
        Deque<LogEntryInlinedCheckPoint> checkPoints = new ArrayDeque<>();
        do
        {
            logEntry = entryReader.readLogEntry( reader );
            if ( logEntry instanceof LogEntryInlinedCheckPoint )
            {
                checkPoints.add( (LogEntryInlinedCheckPoint) logEntry );
            }
        }
        while ( logEntry != null );
        Path highestLogFile = logFile.getLogFileForVersion( logFile.getHighestLogVersion() );
        while ( !checkPoints.isEmpty() )
        {
            LogEntryInlinedCheckPoint checkPoint = checkPoints.pollLast();
            try ( StoreChannel storeChannel = fileSystem.write( highestLogFile.toFile() ) )
            {
                storeChannel.truncate( checkPoint.getLogPosition().getByteOffset() );
            }
            if ( isRecoveryRequired( fileSystem, databaseLayout, Config.defaults(), INSTANCE ) )
            {
                return;
            }
        }
    }
}
