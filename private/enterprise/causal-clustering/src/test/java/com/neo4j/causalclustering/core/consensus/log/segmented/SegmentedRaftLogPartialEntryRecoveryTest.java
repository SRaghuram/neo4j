/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.TokenType;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.NullLogProvider.getInstance;

/**
 * This class tests that partially written entries at the end of the last raft log file (also known as Segment)
 * do not cause a problem. This is guaranteed by rotating after recovery and making sure that half written
 * entries at the end do not stop recovery from proceeding.
 */
@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class SegmentedRaftLogPartialEntryRecoveryTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private File logDirectory;
    private final String databaseName = DEFAULT_DATABASE_NAME;

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        logDirectory = testDirectory.directory();

        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( "100 entries", logProvider ).newInstance();
        return new SegmentedRaftLog( fs, logDirectory, rotateAtSize, ignored -> CoreReplicatedContentMarshalFactory.marshalV1( databaseName ),
                logProvider, 8, Clocks.fakeClock(), new OnDemandJobScheduler(), pruningStrategy );
    }

    private RecoveryProtocol createRecoveryProtocol()
    {
        FileNames fileNames = new FileNames( logDirectory );
        // TODO: Marshal map
        return new RecoveryProtocol( fs, fileNames, new ReaderPool( 8, getInstance(), fileNames, fs, Clocks.fakeClock() ),
                ignored -> CoreReplicatedContentMarshalFactory.marshalV1( databaseName ), getInstance() );
    }

    @Test
    void incompleteEntriesAtTheEndShouldNotCauseFailures() throws Throwable
    {
        // Given
        // we use a RaftLog to create a raft log file and then we will start chopping bits off from the end
        SegmentedRaftLog raftLog = createRaftLog( 100_000 );

        raftLog.start();

        // Add a bunch of entries, preferably one of each available kind.
        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );
        String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
        raftLog.append( new RaftLogEntry( 4, new ReplicatedIdAllocationRequest( new MemberId( UUID.randomUUID() ),
                IdType.RELATIONSHIP, 1, 1024, databaseName ) ) );
        raftLog.append( new RaftLogEntry( 4, new ReplicatedIdAllocationRequest( new MemberId( UUID.randomUUID() ),
                IdType.RELATIONSHIP, 1025, 1024, databaseName ) ) );
        raftLog.append( new RaftLogEntry( 4, new ReplicatedLockTokenRequest( new MemberId( UUID.randomUUID() ), 1, databaseName ) ) );
        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );
        raftLog.append( new RaftLogEntry( 5, new ReplicatedTokenRequest( databaseName, TokenType.LABEL, "labelToken", new byte[]{ 1, 2, 3 } ) ) );
        raftLog.append( new RaftLogEntry( 5,
                ReplicatedTransaction.from( new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9 , 10 }, databaseName ) ) );

        raftLog.stop();

        // We use a temporary RecoveryProtocol to get the file to chop
        RecoveryProtocol recovery = createRecoveryProtocol();
        State recoveryState = recovery.run();
        String logFilename = recoveryState.segments.last().getFilename();
        recoveryState.segments.close();
        File logFile = new File( logDirectory, logFilename );

        // When
        // We remove any number of bytes from the end (up to but not including the header) and try to recover
        // Then
        // No exceptions should be thrown
        truncateAndRecover( logFile, SegmentHeader.CURRENT_RECORD_OFFSET );
    }

    @Test
    void incompleteHeaderOfLastOfMoreThanOneLogFilesShouldNotCauseFailure() throws Throwable
    {
        // Given
        // we use a RaftLog to create two log files, in order to chop the header of the second
        SegmentedRaftLog raftLog = createRaftLog(1 );

        raftLog.start();

        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) ); // will cause rotation

        raftLog.stop();

        // We use a temporary RecoveryProtocol to get the file to chop
        RecoveryProtocol recovery = createRecoveryProtocol();
        State recoveryState = recovery.run();
        String logFilename = recoveryState.segments.last().getFilename();
        recoveryState.segments.close();
        File logFile = new File( logDirectory, logFilename );

        // When
        // We remove any number of bytes from the end of the second file and try to recover
        // Then
        // No exceptions should be thrown
        truncateAndRecover( logFile, 0 );
    }

    @Test
    void shouldNotAppendAtTheEndOfLogFileWithIncompleteEntries() throws Throwable
    {
        // Given
        // we use a RaftLog to create a raft log file and then we will chop some bits off the end
        SegmentedRaftLog raftLog = createRaftLog(100_000 );

        raftLog.start();

        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );

        raftLog.stop();

        // We use a temporary RecoveryProtocol to get the file to chop
        RecoveryProtocol recovery = createRecoveryProtocol();
        State recoveryState = recovery.run();
        String logFilename = recoveryState.segments.last().getFilename();
        recoveryState.segments.close();
        File logFile = new File( logDirectory, logFilename );
        StoreChannel lastFile = fs.create( logFile );
        long currentSize = lastFile.size();
        lastFile.close();

        // When
        // We induce an incomplete entry at the end of the last file
        lastFile = fs.create( logFile );
        lastFile.truncate( currentSize - 1 );
        lastFile.close();

        // We start the raft log again, on the previous log file with truncated last entry.
        raftLog = createRaftLog( 100_000 );

        //  Recovery will run here
        raftLog.start();

        // Append an entry
        raftLog.append( new RaftLogEntry( 4, new NewLeaderBarrier() ) );

        // Then
        // The log should report as containing only the last entry we've appended
        try ( RaftLogCursor entryCursor = raftLog.getEntryCursor( 0 ) )
        {
            // There should be exactly one entry, of type NewLeaderBarrier
            assertTrue( entryCursor.next() );
            RaftLogEntry raftLogEntry = entryCursor.get();
            assertEquals( NewLeaderBarrier.class, raftLogEntry.content().getClass() );
            assertFalse( entryCursor.next() );
        }
        raftLog.stop();
    }

    /**
     * Truncates and recovers the log file provided, one byte at a time until it reaches the header.
     * The reason the header is not truncated (and instead has its own test) is that if the log consists of
     * only one file (Segment) and the header is incomplete, that is correctly an exceptional circumstance and
     * is tested elsewhere.
     */
    private void truncateAndRecover( File logFile, long truncateDownToSize )
            throws IOException, DamagedLogStorageException, DisposedException
    {
        StoreChannel lastFile = fs.create( logFile );
        long currentSize = lastFile.size();
        lastFile.close();
        RecoveryProtocol recovery;
        while ( currentSize-- > truncateDownToSize )
        {
            lastFile = fs.create( logFile );
            lastFile.truncate( currentSize );
            lastFile.close();
            recovery = createRecoveryProtocol();
            State state = recovery.run();
            state.segments.close();
        }
    }
}
