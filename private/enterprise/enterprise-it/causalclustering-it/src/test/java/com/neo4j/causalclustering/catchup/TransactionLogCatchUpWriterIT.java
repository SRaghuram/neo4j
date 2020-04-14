/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.LongStream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.io.FileUtils.sizeOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@EnterpriseDbmsExtension
class TransactionLogCatchUpWriterIT
{
    @Inject
    private TestDirectory dir;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseManagementService dbms;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private StorageEngineFactory storageEngineFactory;
    @Inject
    private StorageEngine storageEngine;

    private final int MANY_TRANSACTIONS = 100_000; // should be somewhere above the rotation threshold

    private DatabaseLayout databaseLayout;
    private StoreId storeId;

    @BeforeEach
    public void setup() throws IOException
    {
        this.databaseLayout = db.databaseLayout();
        this.storeId = storageEngine.getStoreId();

        dbms.shutdownDatabase( db.databaseName() );

        deleteTransactionLogs();
    }

    private void deleteTransactionLogs() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fs ).build();
        logFiles.accept( ( file, version ) -> deleteFileOrThrow( file ) );
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "fullStoreCopy: {0}" )
    void shouldCreateTransactionLogWithCheckpoint( boolean fullStoreCopy ) throws Exception
    {
        createTransactionLogWithCheckpoint( defaults(), true, fullStoreCopy );
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "fullStoreCopy: {0}" )
    void createTransactionLogWithCheckpointInCustomLocation( boolean fullStoreCopy ) throws IOException
    {
        createTransactionLogWithCheckpoint( defaults( transaction_logs_root_path, dir.directory( "custom-tx-logs" ).toPath().toAbsolutePath() ), false,
                fullStoreCopy );
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "fullStoreCopy: {0}" )
    void pullRotatesWhenThresholdCrossed( boolean fullStoreCopy ) throws IOException
    {
        // given
        Config config = defaults( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) );

        // given a writer
        long nextTxId = BASE_TX_ID;

        for ( int i = 0; i < 3; i++ )
        {
            LongRange validInitialTxId = LongRange.range( nextTxId, nextTxId );
            TransactionLogCatchUpWriter writer = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, nullLogProvider(),
                    storageEngineFactory, validInitialTxId, fullStoreCopy, false, PageCacheTracer.NULL );

            // when a bunch of transactions received
            LongStream.range( nextTxId, nextTxId + MANY_TRANSACTIONS )
                      .mapToObj( TransactionLogCatchUpWriterIT::tx )
                      .map( tx -> new TxPullResponse( storeId, tx ) )
                      .forEach( writer::onTxReceived );

            nextTxId = nextTxId + MANY_TRANSACTIONS;
            writer.close();

            // then there was a rotation
            LogFilesBuilder logFilesBuilder = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache );
            LogFiles logFiles = logFilesBuilder.build();
            assertNotEquals( logFiles.getLowestLogVersion(), logFiles.getHighestLogVersion() );

            verifyTransactionsInLog( logFiles, BASE_TX_ID, nextTxId - 1 );
            verifyCheckpointInLog( logFiles, fullStoreCopy );
        }
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "fullStoreCopy: {0}" )
    void doNotPreallocateTxLogsDuringStoreCopy( boolean fullStoreCopy ) throws IOException
    {
        assumeTrue( SystemUtils.IS_OS_LINUX );
        Config config = defaults();
        TransactionLogCatchUpWriter writer = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, LongRange.range( BASE_TX_ID, BASE_TX_ID ), fullStoreCopy, true, NULL );
        writer.close();
        if ( fullStoreCopy )
        {
            assertThat( sizeOf( databaseLayout.getTransactionLogsDirectory() ), lessThanOrEqualTo( 100L ) );
        }
        else
        {
            assertThat( sizeOf( databaseLayout.getTransactionLogsDirectory() ), greaterThanOrEqualTo( logical_log_rotation_threshold.defaultValue() ) );
        }
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "fullStoreCopy: {0}" )
    void noPreallocateTxLogsDuringStoreCopy( boolean fullStoreCopy ) throws IOException
    {
        assumeTrue( !SystemUtils.IS_OS_LINUX );
        Config config = defaults();
        TransactionLogCatchUpWriter writer = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, LongRange.range( BASE_TX_ID, BASE_TX_ID ), fullStoreCopy, true, NULL );
        writer.close();
        assertThat(sizeOf( databaseLayout.getTransactionLogsDirectory() ), lessThanOrEqualTo( 100L ) );
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "fullStoreCopy: {0}" )
    void tracePageCacheAccessInCatchupWriter( boolean fullStoreCopy ) throws IOException
    {
        Config config = defaults( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) );

        long fromTxId = BASE_TX_ID;
        LongRange validRange = LongRange.range( fromTxId, fromTxId );
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( TransactionLogCatchUpWriter subject = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, validRange, fullStoreCopy, false, pageCacheTracer ) )
        {
            LongStream.range( fromTxId, MANY_TRANSACTIONS )
                    .mapToObj( TransactionLogCatchUpWriterIT::tx )
                    .map( tx -> new TxPullResponse( storeId, tx ) )
                    .forEach( subject::onTxReceived );
        }

        if ( fullStoreCopy )
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

    private void createTransactionLogWithCheckpoint( Config config, boolean logsInStoreDir, boolean fullStoreCopy ) throws IOException
    {
        int fromTxId = 37;
        int endTxId = fromTxId + 5;

        LongRange validRange = LongRange.range( fromTxId, fromTxId );

        TransactionLogCatchUpWriter catchUpWriter = new TransactionLogCatchUpWriter( databaseLayout, fs, pageCache, config, NullLogProvider.getInstance(),
                storageEngineFactory, validRange, fullStoreCopy, logsInStoreDir, NULL );

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
        verifyCheckpointInLog( logFiles, fullStoreCopy );
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

    private LogEntryReader logEntryReader()
    {
        return new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
    }

    private void deleteFileOrThrow( File file )
    {
        try
        {
            fs.deleteFileOrThrow( file );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static CommittedTransactionRepresentation tx( long txId )
    {
        StorageCommand command = new Command.NodeCommand( new NodeRecord( 1 ), new NodeRecord( 1 ) );
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( List.of( command ), new byte[0], 0, 0, 0, 0 );
        return new CommittedTransactionRepresentation(
                new LogEntryStart( 0, txId - 1, 0, new byte[]{}, LogPosition.UNSPECIFIED ),
                tx,
                new LogEntryCommit( txId, 0, BASE_TX_CHECKSUM ) );
    }
}
