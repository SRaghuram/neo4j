/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
public abstract class RaftLogVerificationIT
{
    @Inject
    private TestDirectory testDirectory;

    private VerifyingRaftLog raftLog;

    protected abstract RaftLog createRaftLog( TestDirectory testDirectory ) throws Throwable;

    protected abstract long operations();

    @BeforeEach
    void before() throws Throwable
    {
        raftLog = new VerifyingRaftLog( createRaftLog( testDirectory ) );
    }

    @AfterEach
    void after() throws Throwable
    {
        raftLog.verify();
    }

    @Test
    void verifyAppend() throws Throwable
    {
        for ( int i = 0; i < operations(); i++ )
        {
            raftLog.append( new RaftLogEntry( i * 3, valueOf( i * 7 ) ) );
        }
    }

    @Test
    void verifyAppendWithIntermittentTruncation() throws Throwable
    {
        for ( int i = 0; i < operations(); i++ )
        {
            raftLog.append( new RaftLogEntry( i * 3, valueOf( i * 7 ) ) );
            if ( i > 0 && i % 13 == 0 )
            {
                raftLog.truncate( raftLog.appendIndex() - 10 );
            }
        }
    }

    @Test
    void randomAppendAndTruncate() throws Exception
    {
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        // given
        for ( int i = 0; i < operations(); i++ )
        {
            final int finalAppendIndex = tlr.nextInt( 10 ) + 1;
            int appendIndex = finalAppendIndex;
            while ( appendIndex-- > 0 )
            {
                raftLog.append( new RaftLogEntry( i, valueOf( i ) ) );
            }

            int truncateIndex = tlr.nextInt( finalAppendIndex ); // truncate index must be strictly less than append index
            while ( truncateIndex-- > 0 )
            {
                raftLog.truncate( truncateIndex );
            }
        }
    }

    @Test
    void shouldBeAbleToAppendAfterSkip() throws Throwable
    {
        int term = 0;
        raftLog.append( new RaftLogEntry( term, valueOf( 10 ) ) );

        int newTerm = 3;
        raftLog.skip( 5, newTerm );
        RaftLogEntry newEntry = new RaftLogEntry( newTerm, valueOf( 20 ) );
        raftLog.append( newEntry ); // this will be logIndex 6

        assertEquals( newEntry, RaftLogHelper.readLogEntry( raftLog, 6 ) );
    }
}
