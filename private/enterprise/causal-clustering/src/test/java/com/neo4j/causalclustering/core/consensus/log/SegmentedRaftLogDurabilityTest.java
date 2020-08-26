/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import com.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.core.consensus.log.RaftLogHelper.hasNoContent;
import static com.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
class SegmentedRaftLogDurabilityTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final RaftLogFactory logFactory = fileSystem ->
    {
        Path directory = testDirectory.homePath();
        fileSystem.mkdir( directory );

        long rotateAtSizeBytes = 128;
        int readerPoolSize = 8;

        NullLogProvider logProvider = getInstance();
        SegmentedRaftLog log =
                new SegmentedRaftLog( fileSystem, directory, rotateAtSizeBytes, ignored -> new DummyRaftableContentSerializer(),
                        logProvider, readerPoolSize, Clocks.fakeClock(), new OnDemandJobScheduler(),
                        new CoreLogPruningStrategyFactory( "1 size", logProvider ).newInstance(), INSTANCE );
        log.start();

        return log;
    };

    @Test
    void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        final RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fs, myLog ->
        {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntry ) );
        } );
    }

    @Test
    void shouldAppendAndCommit() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fs,
                myLog -> assertThat( myLog.appendIndex(), is( 0L ) ) );
    }

    @Test
    void shouldAppendAfterReloadingFromFileSystem() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntryA );

        fs.crash();
        log = logFactory.createBasedOn( fs );

        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( readLogEntry( log, 0 ), is( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), is( logEntryB ) );
    }

    @Test
    void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.truncate( 1 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fs,
                myLog -> assertThat( myLog.appendIndex(), is( 0L ) ) );
    }

    @Test
    void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        final RaftLogEntry logEntryC = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 3 ) );
        final RaftLogEntry logEntryD = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) );
        final RaftLogEntry logEntryE = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 5 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.append( logEntryC );

        log.truncate( 1 );

        log.append( logEntryD );
        log.append( logEntryE );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fs, myLog ->
        {
            assertThat( myLog.appendIndex(), is( 2L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntryA ) );
            assertThat( readLogEntry( myLog, 1 ), equalTo( logEntryD ) );
            assertThat( readLogEntry( myLog, 2 ), equalTo( logEntryE ) );
        } );
    }

    @Test
    void shouldLogDifferentContentTypes() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA );
        log.append( logEntryB );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fs, myLog ->
        {
            assertThat( myLog.appendIndex(), is( 1L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntryA ) );
            assertThat( readLogEntry( myLog, 1 ), equalTo( logEntryB ) );
        } );
    }

    @Test
    void shouldRecoverAfterEventuallyPruning() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fs );

        long term = 0L;

        long safeIndex;
        long prunedIndex = -1;
        int val = 0;

        // this loop should eventually be able to prune something
        while ( prunedIndex == -1 )
        {
            for ( int i = 0; i < 100; i++ )
            {
                log.append( new RaftLogEntry( term, valueOf( val++ ) ) );
            }
            safeIndex = log.appendIndex() - 50;
            // when
            prunedIndex = log.prune( safeIndex );
        }

        final long finalAppendIndex = log.appendIndex();
        final long finalPrunedIndex = prunedIndex;
        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fs, myLog ->
        {
            assertThat( log, hasNoContent( 0 ) );
            assertThat( log, hasNoContent( finalPrunedIndex ) );
            assertThat( myLog.prevIndex(), equalTo( finalPrunedIndex ) );
            assertThat( myLog.appendIndex(), is( finalAppendIndex ) );
            assertThat( readLogEntry( myLog, finalPrunedIndex + 1 ).content(), equalTo( valueOf( (int) finalPrunedIndex + 1 ) ) );
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            RaftLog log, EphemeralFileSystemAbstraction fileSystem, LogVerifier logVerifier ) throws Exception
    {
        logVerifier.verifyLog( log );
        logVerifier.verifyLog( logFactory.createBasedOn( fs ) );
        fileSystem.crash();
        logVerifier.verifyLog( logFactory.createBasedOn( fs ) );
    }

    private interface RaftLogFactory
    {
        RaftLog createBasedOn( FileSystemAbstraction fileSystem ) throws Exception;
    }

    private interface LogVerifier
    {
        void verifyLog( RaftLog log ) throws IOException;
    }
}
