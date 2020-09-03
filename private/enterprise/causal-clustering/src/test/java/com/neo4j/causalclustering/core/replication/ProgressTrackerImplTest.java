/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.identity.IdFactory;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

class ProgressTrackerImplTest
{
    private final int DEFAULT_TIMEOUT_MS = 15_000;

    private GlobalSession session = new GlobalSession( UUID.randomUUID(), IdFactory.randomRaftMemberId() );
    private DistributedOperation operationA = new DistributedOperation(
            ReplicatedInteger.valueOf( 0 ), session, new LocalOperationId( 0, 0 ) );
    private DistributedOperation operationB = new DistributedOperation(
            ReplicatedInteger.valueOf( 1 ), session, new LocalOperationId( 1, 0 ) );
    private ProgressTrackerImpl tracker = new ProgressTrackerImpl( session );

    @Test
    void shouldReportThatOperationIsNotReplicatedInitially()
    {
        // when
        Progress progress = tracker.start( operationA );

        // then
        Assertions.assertFalse( progress.isReplicated() );
    }

    @Test
    void shouldWaitForReplication() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );

        // when
        long time = System.currentTimeMillis();
        progress.awaitReplication( 10L );

        // then
        time = System.currentTimeMillis() - time ;
        MatcherAssert.assertThat( time, greaterThanOrEqualTo( 10L ) );
    }

    @Test
    void shouldStopWaitingWhenReplicated() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );

        // when
        Thread waiter = replicationEventWaiter( progress );

        // then
        Assertions.assertTrue( waiter.isAlive() );
        Assertions.assertFalse( progress.isReplicated() );

        // when
        tracker.trackReplication( operationA );

        // then
        Assertions.assertTrue( progress.isReplicated() );
        waiter.join( DEFAULT_TIMEOUT_MS );
        Assertions.assertFalse( waiter.isAlive() );
    }

    @Test
    void shouldBeAbleToAbortTracking()
    {
        // when
        tracker.start( operationA );
        // then
        Assertions.assertEquals( 1L, tracker.inProgressCount() );

        // when
        tracker.abort( operationA );
        // then
        Assertions.assertEquals( 0L, tracker.inProgressCount() );
    }

    @Test
    void shouldCheckThatOneOperationDoesNotAffectProgressOfOther()
    {
        // given
        Progress progressA = tracker.start( operationA );
        Progress progressB = tracker.start( operationB );

        // when
        tracker.trackReplication( operationA );

        // then
        Assertions.assertTrue( progressA.isReplicated() );
        Assertions.assertFalse( progressB.isReplicated() );
    }

    @Test
    void shouldTriggerReplicationEvent() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );
        Thread waiter = replicationEventWaiter( progress );

        // when
        tracker.triggerReplicationEvent();

        // then
        Assertions.assertFalse( progress.isReplicated() );
        waiter.join();
        Assertions.assertFalse( waiter.isAlive() );
    }

    @Test
    void shouldGetTrackedResult() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );

        // when
        String result = "result";
        tracker.trackResult( operationA, StateMachineResult.of( result ) );

        // then
        Assertions.assertEquals( result, progress.result().consume() );
    }

    @Test
    void shouldIgnoreOtherSessions()
    {
        // given
        GlobalSession sessionB = new GlobalSession( UUID.randomUUID(), IdFactory.randomRaftMemberId() );
        DistributedOperation aliasUnderSessionB =
                new DistributedOperation( ReplicatedInteger.valueOf( 0 ), sessionB,
                        new LocalOperationId(
                                /* same id/sequence number as operationA */
                                operationA.operationId().localSessionId(),
                                operationA.operationId().sequenceNumber() ) );

        Progress progressA = tracker.start( operationA );

        // when
        tracker.trackReplication( aliasUnderSessionB );
        tracker.trackResult( aliasUnderSessionB, StateMachineResult.of( "result" ) );

        // then
        Assertions.assertFalse( progressA.isReplicated() );
        Assertions.assertNull( progressA.result() );
    }

    private Thread replicationEventWaiter( Progress progress )
    {
        Thread waiter = new Thread( () ->
        {
            try
            {
                progress.awaitReplication( DEFAULT_TIMEOUT_MS );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        } );

        waiter.start();
        return waiter;
    }
}
