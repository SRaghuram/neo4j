/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import com.neo4j.tools.dump.TransactionLogAnalyzer.Monitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.PrimitiveLongArrayQueue;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@Neo4jLayoutExtension
@ExtendWith( { LifeExtension.class, RandomExtension.class} )
class TransactionLogAnalyzerTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private LifeSupport life;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private RandomRule random;
    private LogFile logFile;
    private FlushablePositionAwareChecksumChannel writer;
    private TransactionLogWriter transactionLogWriter;
    private AtomicLong lastCommittedTxId;
    private VerifyingMonitor monitor;
    private LogVersionRepository logVersionRepository;
    private LogFiles logFiles;

    @BeforeEach
    void before() throws IOException
    {
        lastCommittedTxId = new AtomicLong( BASE_TX_ID );
        logVersionRepository = new SimpleLogVersionRepository();
        logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withRotationThreshold( ByteUnit.mebiBytes( 1 ) )
                .withStoreId( StoreId.UNKNOWN )
                .withCommandReaderFactory( RecordStorageCommandReaderFactory.INSTANCE )
                .build();
        life.add( logFiles );
        logFile = logFiles.getLogFile();
        writer = logFile.getWriter();
        transactionLogWriter = new TransactionLogWriter( new LogEntryWriter( writer ) );
        monitor = new VerifyingMonitor();
    }

    @AfterEach
    void after()
    {
        life.shutdown();
    }

    @Test
    void shouldSeeTransactionsInOneLogFile() throws Exception
    {
        // given
        writeTransactions( 5 );

        // when
        analyzeAllTransactionLogs();

        // then
        assertEquals( 1, monitor.logFiles );
        assertEquals( 5, monitor.transactions );
    }

    @Test
    void throwExceptionWithErrorMessageIfLogFilesNotFound() throws Exception
    {
        File emptyDirectory = testDirectory.directory( "empty" );
        assertThrows( IllegalStateException.class, () -> TransactionLogAnalyzer.analyze( fs, emptyDirectory, monitor ) );
    }

    @Test
    void shouldSeeCheckpointsInBetweenTransactionsInOneLogFile() throws Exception
    {
        // given
        writeTransactions( 3 ); // txs 2, 3, 4
        writeCheckpoint();
        writeTransactions( 2 ); // txs 5, 6
        writeCheckpoint();
        writeTransactions( 4 ); // txs 7, 8, 9, 10

        // when
        analyzeAllTransactionLogs();

        // then
        assertEquals( 1, monitor.logFiles );
        assertEquals( 2, monitor.checkpoints );
        assertEquals( 9, monitor.transactions );
    }

    @Test
    void shouldSeeLogFileTransitions() throws Exception
    {
        // given
        writeTransactions( 1 );
        rotate();
        writeTransactions( 1 );
        rotate();
        writeTransactions( 1 );

        // when
        analyzeAllTransactionLogs();

        // then
        assertEquals( 3, monitor.logFiles );
        assertEquals( 0, monitor.checkpoints );
        assertEquals( 3, monitor.transactions );
    }

    @Test
    void shouldSeeLogFileTransitionsTransactionsAndCheckpointsInMultipleLogFiles() throws Exception
    {
        // given
        int expectedTransactions = 0;
        int expectedCheckpoints = 0;
        int expectedLogFiles = 1;
        for ( int i = 0; i < 30; i++ )
        {
            float chance = random.nextFloat();
            if ( chance < 0.5 )
            {   // tx
                int count = random.nextInt( 1, 5 );
                writeTransactions( count );
                expectedTransactions += count;
            }
            else if ( chance < 0.75 )
            {   // checkpoint
                writeCheckpoint();
                expectedCheckpoints++;
            }
            else
            {   // rotate
                rotate();
                expectedLogFiles++;
            }
        }
        writer.prepareForFlush().flush();

        // when
        analyzeAllTransactionLogs();

        // then
        assertEquals( expectedLogFiles, monitor.logFiles );
        assertEquals( expectedCheckpoints, monitor.checkpoints );
        assertEquals( expectedTransactions, monitor.transactions );
    }

    @Test
    void shouldAnalyzeSingleLogWhenExplicitlySelected() throws Exception
    {
        // given
        writeTransactions( 2 ); // txs 2, 3
        long version = rotate();
        writeTransactions( 3 ); // txs 4, 5, 6
        writeCheckpoint();
        writeTransactions( 4 ); // txs 7, 8, 9, 10
        rotate();
        writeTransactions( 2 ); // txs 11, 12

        // when
        monitor.nextExpectedTxId = 4;
        monitor.nextExpectedLogVersion = version;
        TransactionLogAnalyzer.analyze( fs, logFiles.getLogFileForVersion( version ), monitor );

        // then
        assertEquals( 1, monitor.logFiles );
        assertEquals( 1, monitor.checkpoints );
        assertEquals( 7, monitor.transactions );
    }

    private void analyzeAllTransactionLogs() throws IOException
    {
        TransactionLogAnalyzer.analyze( fs, databaseLayout.getTransactionLogsDirectory().toFile(), monitor );
    }

    private long rotate() throws IOException
    {
        logFile.rotate();
        return logVersionRepository.getCurrentLogVersion();
    }

    private static void assertTransaction( LogEntry[] transactionEntries, long expectedId )
    {
        assertTrue( transactionEntries[0] instanceof LogEntryStart, Arrays.toString( transactionEntries ) );
        assertTrue( transactionEntries[1] instanceof LogEntryCommand );
        LogEntryCommand command = (LogEntryCommand) transactionEntries[1];
        assertEquals( expectedId, ((Command.NodeCommand)command.getCommand()).getKey() );
        assertTrue( transactionEntries[2] instanceof LogEntryCommit );
        LogEntryCommit commit = (LogEntryCommit) transactionEntries[2];
        assertEquals( expectedId, commit.getTxId() );
    }

    private void writeCheckpoint() throws IOException
    {
        transactionLogWriter.checkPoint( writer.getCurrentPosition( new LogPositionMarker() ).newPosition() );
        monitor.expectCheckpointAfter( lastCommittedTxId.get() );
    }

    private void writeTransactions( int count ) throws IOException
    {
        int previousChecksum = BASE_TX_CHECKSUM;
        for ( int i = 0; i < count; i++ )
        {
            long id = lastCommittedTxId.incrementAndGet();
            previousChecksum = transactionLogWriter.append( tx( id ), id, previousChecksum );
        }
        writer.prepareForFlush().flush();
    }

    private TransactionRepresentation tx( long nodeId )
    {
        List<StorageCommand> commands = new ArrayList<>();
        commands.add( new Command.NodeCommand( new NodeRecord( nodeId ), new NodeRecord( nodeId )
                .initialize( true, nodeId, false, nodeId, 0 ) ) );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0 );
        return tx;
    }

    private static class VerifyingMonitor implements Monitor
    {
        private int transactions;
        private int checkpoints;
        private int logFiles;
        private long nextExpectedTxId = BASE_TX_ID + 1;
        private final PrimitiveLongArrayQueue expectedCheckpointsAt = new PrimitiveLongArrayQueue();
        private long nextExpectedLogVersion = BASE_TX_LOG_VERSION;

        void expectCheckpointAfter( long txId )
        {
            expectedCheckpointsAt.enqueue( txId );
        }

        @Override
        public void logFile( File file, long logVersion )
        {
            logFiles++;
            assertEquals( nextExpectedLogVersion++, logVersion );
        }

        @Override
        public void transaction( LogEntry[] transactionEntries )
        {
            transactions++;
            assertTransaction( transactionEntries, nextExpectedTxId++ );
        }

        @Override
        public void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
        {
            checkpoints++;
            Long expected = expectedCheckpointsAt.dequeue();
            assertNotNull( expected, "Unexpected checkpoint" );
            assertEquals( expected.longValue(), nextExpectedTxId - 1 );
        }
    }
}
