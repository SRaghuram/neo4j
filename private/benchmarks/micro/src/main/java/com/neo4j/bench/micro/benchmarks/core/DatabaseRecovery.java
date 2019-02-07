/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.StringGenerator;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.io.File;
import java.io.IOException;
import java.util.SplittableRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.bench.micro.data.DataGenerator.GraphWriter.TRANSACTIONAL;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

public class DatabaseRecovery extends AbstractCoreBenchmark
{
    @ParamValues(
            allowed = {"10000", "100000", "1000000"},
            base = {"10000", "100000"} )
    @Param( {} )
    public int DatabaseRecovery_transactionsToRecover;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String DatabaseRecovery_storeFormat;

    @Override
    protected DataGeneratorConfig getConfig()
    {
        Neo4jConfig neo4jConfig = Neo4jConfig
                .empty()
                .withSetting( record_format, DatabaseRecovery_storeFormat )
                .withSetting( GraphDatabaseSettings.check_point_interval_tx, String.valueOf( Integer.MAX_VALUE ) );
        return new DataGeneratorConfigBuilder()
                .withGraphWriter( TRANSACTIONAL )
                .withNeo4jConfig( neo4jConfig )
                .isReusableStore( false )
                .build();
    }

    private Long expectedNumberOfRecoveredTransactions;

    @Override
    protected void afterDatabaseStart()
    {
        GraphDatabaseService db = managedStore.db();

        GraphDatabaseAPI graphDatabaseService = (GraphDatabaseAPI) db;
        TransactionIdStore transactionIdStore = graphDatabaseService
                .getDependencyResolver()
                .resolveDependency( TransactionIdStore.class );
        long transactionIdBefore = transactionIdStore.getLastCommittedTransactionId();

        ValueGeneratorFun<String> stringGenerator = StringGenerator.randShortAlphaNumerical().create();
        SplittableRandom rng = RNGState.newRandom( 42 );
        for ( int i = 0; i < DatabaseRecovery_transactionsToRecover; i++ )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                Node nodeA = db.createNode();
                Node nodeB = db.createNode();
                String type = String.valueOf( i % 10 );
                nodeA.setProperty( type, stringGenerator.next( rng ) );
                nodeA.createRelationshipTo( nodeB, RelationshipType.withName( type ) );
                transaction.success();
            }
        }

        long transactionIdAfter = transactionIdStore.getLastCommittedTransactionId();
        expectedNumberOfRecoveredTransactions = transactionIdAfter - transactionIdBefore;
    }

    @Setup( Level.Iteration )
    public void truncateCheckpointFromLogs() throws IOException
    {
        managedStore.db().shutdown();
        removeLastCheckpointRecordFromLastLogFile();
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

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        File storeDir = managedStore.store().toFile();
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDir, fileSystem ).build();
        LogPosition checkpointPosition = null;

        LogFile logFile = logFiles.getLogFile();
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();
        ReadableLogChannel reader =
                logFile.getReader( LogPosition.start( logFiles.getHighestLogVersion() ) );
        LogEntry logEntry;
        int checkPointCount = 0;
        do
        {
            logEntry = entryReader.readLogEntry( reader );
            if ( logEntry instanceof CheckPoint )
            {
                checkpointPosition = ((CheckPoint) logEntry).getLogPosition();
                checkPointCount++;
            }
        }
        while ( logEntry != null );
        if ( checkPointCount != 3 )
        {
            throw new RuntimeException( "Expected 3 checkpoint but found " + checkPointCount );
        }
        if ( checkpointPosition != null )
        {
            File highestLogFile = logFiles.getLogFileForVersion( logFiles.getHighestLogVersion() );
            try ( StoreChannel storeChannel = fileSystem.open( highestLogFile, OpenMode.READ_WRITE ) )
            {
                storeChannel.truncate( checkpointPosition.getByteOffset() );
            }
        }
    }
}
