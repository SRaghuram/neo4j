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
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;

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
        GraphDatabaseAPI graphDatabaseService = (GraphDatabaseAPI) managedStore.startDb();
        StartupStatistics startupStatistics = graphDatabaseService
                .getDependencyResolver()
                .resolveDependency( StartupStatistics.class );
        int numberOfRecoveredTransactions = startupStatistics.numberOfRecoveredTransactions();
        if ( numberOfRecoveredTransactions != expectedNumberOfRecoveredTransactions )
        {
            throw new RuntimeException( "Recovered unexpected number of transactions" );
        }
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
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fileSystem );
        PhysicalFilesBasedLogVersionRepository versionRepository =
                new PhysicalFilesBasedLogVersionRepository( logFiles );
        LogPosition checkpointPosition = null;
        try ( Lifespan lifespan = new Lifespan() )
        {
            PhysicalLogFile physicalLogFile = new PhysicalLogFile(
                    fileSystem,
                    logFiles,
                    Long.MAX_VALUE,
                    () -> Long.MAX_VALUE,
                    versionRepository,
                    PhysicalLogFile.NO_MONITOR,
                    new LogHeaderCache( 100 ) );
            lifespan.add( physicalLogFile );
            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();
            ReadableLogChannel reader =
                    physicalLogFile.getReader( LogPosition.start( logFiles.getHighestLogVersion() ) );
            LogEntry logEntry;
            do
            {
                logEntry = entryReader.readLogEntry( reader );
                if ( logEntry instanceof CheckPoint )
                {
                    checkpointPosition = ((CheckPoint) logEntry).getLogPosition();
                }
            }
            while ( logEntry != null );
        }
        if ( checkpointPosition != null )
        {
            File highestLogFile = logFiles.getLogFileForVersion( logFiles.getHighestLogVersion() );
            try ( StoreChannel storeChannel = fileSystem.open( highestLogFile, "rw" ) )
            {
                storeChannel.truncate( checkpointPosition.getByteOffset() );
            }
        }
    }

    private class PhysicalFilesBasedLogVersionRepository implements LogVersionRepository
    {
        private long version;

        PhysicalFilesBasedLogVersionRepository( PhysicalLogFiles logFiles )
        {
            this.version = (logFiles.getHighestLogVersion() == -1) ? 0 : logFiles.getHighestLogVersion();
        }

        @Override
        public long getCurrentLogVersion()
        {
            return version;
        }

        @Override
        public long incrementAndGetVersion()
        {
            version++;
            return version;
        }
    }
}
