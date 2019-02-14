/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.identity.StoreId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.api.TestCommandReaderFactory.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
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
        databaseLayout = dir.databaseLayout();
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
        createTransactionLogWithCheckpoint( defaults( transaction_logs_root_path, dir.directory( "custom-tx-logs" ).getAbsolutePath() ), false );
    }

    @Test
    public void pullRotatesWhenThresholdCrossedAndExplicitlySet() throws IOException
    {
        // given
        Config config = defaults();
        config.augment( GraphDatabaseSettings.logical_log_rotation_threshold, "1M" ); // 1 mebibyte

        // and
        org.neo4j.storageengine.api.StoreId storeId = simulateStoreCopy();

        // and
        long fromTxId = BASE_TX_ID;
        TransactionLogCatchUpWriter subject =
                new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(), storageEngineFactory, fromTxId,
                        partOfStoreCopy, false, true );

        // when a bunch of transactions received
        LongStream.range( fromTxId, MANY_TRANSACTIONS )
                .mapToObj( TransactionLogCatchUpWriterTest::tx )
                .map( tx -> new TxPullResponse( toCasualStoreId( storeId ), tx ) )
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
    public void pullDoesNotRotateWhenThresholdCrossedAndExplicitlyOff() throws IOException
    {
        // given
        Config config = defaults();
        config.augment( GraphDatabaseSettings.logical_log_rotation_threshold, "1M" ); // 1 mebibyte

        // and
        org.neo4j.storageengine.api.StoreId storeId = simulateStoreCopy();

        // and
        long fromTxId = BASE_TX_ID;
        TransactionLogCatchUpWriter subject =
                new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(), storageEngineFactory, fromTxId,
                        partOfStoreCopy, false, false );

        // when 1M tx received
        LongStream.range( fromTxId, MANY_TRANSACTIONS )
                .mapToObj( TransactionLogCatchUpWriterTest::tx )
                .map(tx -> new TxPullResponse( toCasualStoreId( storeId ), tx ))
                .forEach( subject::onTxReceived );
        subject.close();

        // then there was a rotation
        LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
        LogFiles logFiles = logFilesBuilder.build();
        assertEquals( logFiles.getLowestLogVersion(), logFiles.getHighestLogVersion() );
    }

    private void createTransactionLogWithCheckpoint( Config config, boolean logsInStoreDir ) throws IOException
    {
        org.neo4j.storageengine.api.StoreId storeId = simulateStoreCopy();

        int fromTxId = 37;
        int endTxId = fromTxId + 5;

        TransactionLogCatchUpWriter catchUpWriter = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config,
                NullLogProvider.getInstance(), storageEngineFactory, fromTxId, partOfStoreCopy, logsInStoreDir, true );

        // when
        for ( int i = fromTxId; i <= endTxId; i++ )
        {
            catchUpWriter.onTxReceived( new TxPullResponse( toCasualStoreId( storeId ), tx( i ) ) );
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
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = logEntryReader();
        final LogTailScanner logTailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );

        LogTailInformation tailInformation = logTailScanner.getTailInformation();

        if ( !shouldExist )
        {
            assertNull( tailInformation.lastCheckPoint );
            return;
        }

        assertNotNull( tailInformation.lastCheckPoint );
        assertEquals( 0, tailInformation.lastCheckPoint.getLogPosition().getLogVersion() );
        assertEquals( LOG_HEADER_SIZE, tailInformation.lastCheckPoint.getLogPosition().getByteOffset() );
        assertTrue( tailInformation.commitsAfterLastCheckpoint() );
    }

    private void verifyTransactionsInLog( LogFiles logFiles, long fromTxId, long endTxId ) throws
            IOException
    {
        long expectedTxId = fromTxId;
        LogVersionedStoreChannel versionedStoreChannel = logFiles.openForVersion( 0 );
        try ( ReadableLogChannel channel =
                      new ReadAheadLogChannel( versionedStoreChannel, LogVersionBridge.NO_MORE_CHANNELS, 1024 ) )
        {
            try ( PhysicalTransactionCursor<ReadableLogChannel> txCursor = new PhysicalTransactionCursor<>( channel, logEntryReader() ) )
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

    private org.neo4j.storageengine.api.StoreId simulateStoreCopy() throws IOException
    {
        // create an empty store
        org.neo4j.storageengine.api.StoreId storeId;
        Database ds = dsRule.getDatabase( databaseLayout, fs, pageCache );
        storageEngineFactory = ds.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        try ( Lifespan ignored = new Lifespan( ds ) )
        {
            storeId = ds.getStoreId();
        }

        // we don't have log files after a store copy
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fsRule.get() ).build();
        //noinspection ResultOfMethodCallIgnored
        logFiles.accept( ( file, version ) -> file.delete() );

        return storeId;
    }

    private StoreId toCasualStoreId( org.neo4j.storageengine.api.StoreId storeId )
    {
        return new StoreId( storeId.getCreationTime(), storeId.getRandomId(), storeId.getUpgradeTime(), storeId.getUpgradeId() );
    }

    private static CommittedTransactionRepresentation tx( long txId )
    {
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.singletonList( new TestCommand() ), new byte[0], 0, 0, 0, 0, 0, 0 );
        return new CommittedTransactionRepresentation(
                new LogEntryStart( 0, 0, 0, txId - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                tx, new LogEntryCommit( txId, 0 ) );
    }
}
