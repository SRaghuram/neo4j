/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.apache.commons.io.FileUtils.sizeOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@RunWith( Parameterized.class )
public class TransactionLogCatchUpWriterTest
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public DatabaseRule dsRule = new DatabaseRule();

    @Parameterized.Parameter
    public boolean partOfStoreCopy;
    private StorageEngineFactory storageEngineFactory;

    @Parameterized.Parameters( name = "Part of store copy: {0}" )
    public static List<Boolean> partOfStoreCopy()
    {
        return Arrays.asList( Boolean.TRUE, Boolean.FALSE );
    }

    private final int MANY_TRANSACTIONS = 100_000; // should be somewhere above the rotation threshold

    private PageCache pageCache;
    private FileSystemAbstraction fs;
    private DatabaseLayout databaseLayout;

    @Before
    public void setup()
    {

        databaseLayout = DatabaseLayout.of( Config.defaults( neo4j_home, dir.homeDir().toPath() ) );
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    @Test
    public void shouldCreateTransactionLogWithCheckpoint() throws Exception
    {
        createTransactionLogWithCheckpoint( defaults(), true );
    }

    @Test
    public void createTransactionLogWithCheckpointInCustomLocation() throws IOException
    {
        createTransactionLogWithCheckpoint( defaults( transaction_logs_root_path, dir.directory( "custom-tx-logs" ).toPath().toAbsolutePath() ), false );
    }

    @Test
    public void pullRotatesWhenThresholdCrossedAndExplicitlySet() throws IOException
    {
        // given
        Config config = defaults( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) );

        // and
        StoreId storeId = simulateStoreCopy();

        // and
        long fromTxId = BASE_TX_ID;
        LongRange validRange = LongRange.range( fromTxId, fromTxId );
        TransactionLogCatchUpWriter subject =
                new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                        storageEngineFactory, validRange, partOfStoreCopy, false, NULL );

        // when a bunch of transactions received
        LongStream.range( fromTxId, MANY_TRANSACTIONS )
                .mapToObj( TransactionLogCatchUpWriterTest::tx )
                .map( tx -> new TxPullResponse( storeId, tx ) )
                .forEach( subject::onTxReceived );
        subject.close();

        // then there was a rotation
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
        LogFiles logFiles = logFilesBuilder.build();
        assertNotEquals( logFiles.getLowestLogVersion(), logFiles.getHighestLogVersion() );
        verifyTransactionsInLog( logFiles, fromTxId, MANY_TRANSACTIONS );
        verifyCheckpointInLog( logFiles, partOfStoreCopy );
    }

    @Test
    public void doNotPreallocateTxLogsDuringStoreCopy() throws IOException
    {
        assumeTrue( SystemUtils.IS_OS_LINUX );
        Config config = defaults();
        simulateStoreCopy();
        TransactionLogCatchUpWriter writer = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, LongRange.range( BASE_TX_ID, BASE_TX_ID ), partOfStoreCopy, true, NULL );
        writer.close();
        if ( partOfStoreCopy )
        {
            assertThat( sizeOf( databaseLayout.getTransactionLogsDirectory() ), lessThanOrEqualTo( 100L ) );
        }
        else
        {
            assertThat( sizeOf( databaseLayout.getTransactionLogsDirectory() ), greaterThanOrEqualTo( logical_log_rotation_threshold.defaultValue() ) );
        }
    }

    @Test
    public void noPreallocateTxLogsDuringStoreCopy() throws IOException
    {
        assumeTrue( !SystemUtils.IS_OS_LINUX );
        Config config = defaults();
        simulateStoreCopy();
        TransactionLogCatchUpWriter writer = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, LongRange.range( BASE_TX_ID, BASE_TX_ID ), partOfStoreCopy, true, NULL );
        writer.close();
        assertThat(sizeOf( databaseLayout.getTransactionLogsDirectory() ), lessThanOrEqualTo( 100L ) );
    }

    @Test
    public void tracePageCacheAccessInCatchupWriter() throws IOException
    {
        Config config = defaults( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) );
        StoreId storeId = simulateStoreCopy();

        long fromTxId = BASE_TX_ID;
        LongRange validRange = LongRange.range( fromTxId, fromTxId );
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( TransactionLogCatchUpWriter subject = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, validRange, partOfStoreCopy, false, pageCacheTracer ) )
        {
            LongStream.range( fromTxId, MANY_TRANSACTIONS )
                    .mapToObj( TransactionLogCatchUpWriterTest::tx )
                    .map( tx -> new TxPullResponse( storeId, tx ) )
                    .forEach( subject::onTxReceived );
        }

        if ( partOfStoreCopy )
        {
            assertEquals( 17L, pageCacheTracer.pins() );
            assertEquals( 17L, pageCacheTracer.unpins() );
            assertEquals( 12L, pageCacheTracer.hits() );
            assertEquals( 5L, pageCacheTracer.faults() );
        }
        else
        {
            assertEquals( 12L, pageCacheTracer.pins() );
            assertEquals( 12L, pageCacheTracer.unpins() );
            assertEquals( 7L, pageCacheTracer.hits() );
            assertEquals( 5L, pageCacheTracer.faults() );
        }
    }

    private void createTransactionLogWithCheckpoint( Config config, boolean logsInStoreDir ) throws IOException
    {
        StoreId storeId = simulateStoreCopy();

        int fromTxId = 37;
        int endTxId = fromTxId + 5;

        LongRange validRange = LongRange.range( fromTxId, fromTxId );

        TransactionLogCatchUpWriter catchUpWriter = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, validRange, partOfStoreCopy, logsInStoreDir, NULL );

        // when
        for ( int i = fromTxId; i <= endTxId; i++ )
        {
            catchUpWriter.onTxReceived( new TxPullResponse( storeId, tx( i ) ) );
        }

        catchUpWriter.close();

        // then
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
        if ( !logsInStoreDir )
        {
            logFilesBuilder.withConfig( config );
        }
        LogFiles logFiles = logFilesBuilder.build();

        verifyTransactionsInLog( logFiles, fromTxId, endTxId );
        verifyCheckpointInLog( logFiles, partOfStoreCopy );
    }

    private void verifyCheckpointInLog( LogFiles logFiles, boolean shouldExist )
    {
        final LogTailScanner logTailScanner = new LogTailScanner( logFiles, logEntryReader(), new Monitors() );

        LogTailInformation tailInformation = logTailScanner.getTailInformation();

        if ( !shouldExist )
        {
            assertNull( tailInformation.lastCheckPoint );
            return;
        }

        assertNotNull( tailInformation.lastCheckPoint );
        assertEquals( 0, tailInformation.lastCheckPoint.getLogPosition().getLogVersion() );
        assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE, tailInformation.lastCheckPoint.getLogPosition().getByteOffset() );
        assertTrue( tailInformation.commitsAfterLastCheckpoint() );
    }

    private void verifyTransactionsInLog( LogFiles logFiles, long fromTxId, long endTxId ) throws
            IOException
    {
        long expectedTxId = fromTxId;
        LogVersionedStoreChannel versionedStoreChannel = logFiles.openForVersion( 0 );
        try ( ReadableLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel ) )
        {
            try ( PhysicalTransactionCursor txCursor = new PhysicalTransactionCursor( channel, logEntryReader() ) )
            {
                while ( txCursor.next() )
                {
                    CommittedTransactionRepresentation tx = txCursor.get();
                    long txId = tx.getCommitEntry().getTxId();

                    assertThat( expectedTxId, lessThanOrEqualTo( endTxId ) );
                    assertEquals( expectedTxId, txId );
                    expectedTxId++;
                }
            }
        }
    }

    private StoreId simulateStoreCopy() throws IOException
    {
        // create an empty store
        StoreId storeId;
        Database ds = dsRule.getDatabase( databaseLayout, fs, pageCache );
        ds.start();
        storageEngineFactory = ds.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        try ( Lifespan ignored = new Lifespan( ds ) )
        {
            storeId = ds.getStoreId();
        }

        // we don't have log files after a store copy
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fsRule.get() ).build();
        //noinspection ResultOfMethodCallIgnored
        logFiles.accept( ( file, version ) -> file.delete() );

        return storeId;
    }

    private static CommittedTransactionRepresentation tx( long txId )
    {
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.singletonList( new TestCommand() ), new byte[0], 0, 0, 0, 0 );
        return new CommittedTransactionRepresentation(
                new LogEntryStart( 0, txId - 1, 0, new byte[]{}, LogPosition.UNSPECIFIED ),
                tx, new LogEntryCommit( txId, 0, BASE_TX_CHECKSUM ) );
    }
}
