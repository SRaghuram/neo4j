/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.helper.scheduling.QueueingScheduler;
import com.neo4j.causalclustering.helper.scheduling.ReoccurringJobQueue;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.test.OnDemandJobScheduler;

import static com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import static com.neo4j.causalclustering.core.consensus.RaftMessages.Heartbeat;
import static com.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry;
import static com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.internal.helpers.ArrayUtil.lastOf;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.LogAssertions.assertThat;

class BatchingMessageHandlerTest
{
    private static final BoundedPriorityQueue.Config IN_QUEUE_CONFIG = new BoundedPriorityQueue.Config( 64, 1024 );
    private static final BatchingConfig BATCH_CONFIG = new BatchingConfig( 16, 256 );
    private final Instant now = Instant.now();
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<InboundRaftMessageContainer<?>> downstreamHandler = mock( LifecycleMessageHandler.class );
    private RaftId localRaftId = RaftIdFactory.random();
    private final QueueingScheduler jobScheduler = mock( QueueingScheduler.class );

    private ExecutorService executor;
    private MemberId leader = new MemberId( UUID.randomUUID() );

    @BeforeEach
    void before()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void after() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 60, TimeUnit.SECONDS );
    }

    @Test
    void shouldInvokeInnerHandlerWhenRun()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        NewEntry.Request message = new NewEntry.Request( null, content( "dummy" ) );

        batchHandler.handle( wrap( message ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        NewEntry.BatchRequest expected = new NewEntry.BatchRequest( singletonList( new ReplicatedString( "dummy" ) ) );
        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    void shouldInvokeHandlerOnQueuedMessage()
    {
        // given
        var scheduler = new OnDemandJobScheduler();
        var queueingScheduler = new QueueingScheduler( scheduler, Group.RAFT_BATCH_HANDLER, NullLog.getInstance(), new ReoccurringJobQueue<>() );
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, queueingScheduler, NullLogProvider.getInstance() );
        ReplicatedString content = new ReplicatedString( "dummy" );
        NewEntry.Request message = new NewEntry.Request( null, content );

        // when
        batchHandler.handle( wrap( message ) );
        scheduler.runJob();

        // then
        NewEntry.BatchRequest expected = new NewEntry.BatchRequest( singletonList( content ) );
        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    void shouldBatchRequests()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );
        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentB = new ReplicatedString( "B" );
        NewEntry.Request messageA = new NewEntry.Request( null, contentA );
        NewEntry.Request messageB = new NewEntry.Request( null, contentB );

        batchHandler.handle( wrap( messageA ) );
        batchHandler.handle( wrap( messageB ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        NewEntry.BatchRequest expected = new NewEntry.BatchRequest( asList( contentA, contentB ) );
        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    void shouldBatchUsingReceivedInstantOfFirstReceivedMessage()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );
        ReplicatedString content = new ReplicatedString( "A" );
        NewEntry.Request messageA = new NewEntry.Request( null, content );

        Instant firstReceived = Instant.ofEpochMilli( 1L );
        Instant secondReceived = firstReceived.plusMillis( 1L );

        batchHandler.handle( wrap( firstReceived, messageA ) );
        batchHandler.handle( wrap( secondReceived, messageA ) );

        // when
        batchHandler.run();

        // then
        NewEntry.BatchRequest batchRequest = new NewEntry.BatchRequest( asList( content, content ) );
        verify( downstreamHandler ).handle( wrap( firstReceived, batchRequest ) );
    }

    @Test
    void shouldBatchNewEntriesAndHandleOtherMessagesFirst()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentC = new ReplicatedString( "C" );

        NewEntry.Request newEntryA = new NewEntry.Request( null, contentA );
        Heartbeat heartbeatA = new Heartbeat( null, 0, 0, 0 );
        NewEntry.Request newEntryB = new NewEntry.Request( null, contentC );
        Heartbeat heartbeatB = new Heartbeat( null, 1, 1, 1 );

        batchHandler.handle( wrap( newEntryA ) );
        batchHandler.handle( wrap( heartbeatA ) );
        batchHandler.handle( wrap( newEntryB ) );
        batchHandler.handle( wrap( heartbeatB ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run(); // heartbeatA
        batchHandler.run(); // heartbeatB
        batchHandler.run(); // batchRequest

        // then
        NewEntry.BatchRequest batchRequest = new NewEntry.BatchRequest( asList( contentA, contentC ) );

        verify( downstreamHandler ).handle( wrap( heartbeatA ) );
        verify( downstreamHandler ).handle( wrap( heartbeatB ) );
        verify( downstreamHandler ).handle( wrap( batchRequest ) );
    }

    @Test
    void shouldBatchSingleEntryAppendEntries()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        long leaderTerm = 1;
        long prevLogIndex = -1;
        long prevLogTerm = -1;
        long leaderCommit = 0;

        RaftLogEntry entryA = new RaftLogEntry( 0, content( "A" ) );
        RaftLogEntry entryB = new RaftLogEntry( 0, content( "B" ) );

        AppendEntries.Request appendA = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                new RaftLogEntry[]{entryA}, leaderCommit );

        AppendEntries.Request appendB = new AppendEntries.Request( leader, leaderTerm, prevLogIndex + 1, 0,
                new RaftLogEntry[]{entryB}, leaderCommit );

        batchHandler.handle( wrap( appendA ) );
        batchHandler.handle( wrap( appendB ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        AppendEntries.Request expected = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                new RaftLogEntry[]{entryA, entryB}, leaderCommit );

        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    void shouldBatchMultipleEntryAppendEntries()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        long leaderTerm = 1;
        long prevLogIndex = -1;
        long prevLogTerm = -1;
        long leaderCommit = 0;

        RaftLogEntry[] entriesA = entries( 0, 0, 2 );
        RaftLogEntry[] entriesB = entries( 1, 3, 3 );
        RaftLogEntry[] entriesC = entries( 2, 4, 8 );
        RaftLogEntry[] entriesD = entries( 3, 9, 15 );

        AppendEntries.Request appendA = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                entriesA, leaderCommit );

        prevLogIndex += appendA.entries().length;
        prevLogTerm = lastOf( appendA.entries() ).term();
        leaderCommit += 2; // arbitrary

        AppendEntries.Request appendB = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                entriesB, leaderCommit );

        prevLogIndex += appendB.entries().length;
        prevLogTerm = lastOf( appendB.entries() ).term();
        leaderCommit += 5; // arbitrary

        AppendEntries.Request appendC = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                ArrayUtil.concat( entriesC, entriesD ), leaderCommit );

        batchHandler.handle( wrap( appendA ) );
        batchHandler.handle( wrap( appendB ) );
        batchHandler.handle( wrap( appendC ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        AppendEntries.Request expected = new AppendEntries.Request( leader, leaderTerm, -1, -1,
                ArrayUtil.concatArrays( entriesA, entriesB, entriesC, entriesD ), leaderCommit );

        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    void shouldNotBatchAppendEntriesDifferentLeaderTerms()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        long leaderTerm = 1;
        long prevLogIndex = -1;
        long prevLogTerm = -1;
        long leaderCommit = 0;

        RaftLogEntry[] entriesA = entries( 0, 0, 2 );
        RaftLogEntry[] entriesB = entries( 1, 3, 3 );

        AppendEntries.Request appendA = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                entriesA, leaderCommit );

        prevLogIndex += appendA.entries().length;
        prevLogTerm = lastOf( appendA.entries() ).term();

        AppendEntries.Request appendB = new AppendEntries.Request( leader, leaderTerm + 1, prevLogIndex, prevLogTerm,
                entriesB, leaderCommit );

        batchHandler.handle( wrap( appendA ) );
        batchHandler.handle( wrap( appendB ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run();
        batchHandler.run();

        // then
        verify( downstreamHandler ).handle( wrap( appendA ) );
        verify( downstreamHandler ).handle( wrap( appendB ) );
    }

    @Test
    void shouldPrioritiseCorrectly()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        NewEntry.Request newEntry = new NewEntry.Request( null, content( "" ) );
        AppendEntries.Request append = new AppendEntries.Request( leader, 1, -1, -1,
                entries( 0, 0, 0 ), 0 );
        AppendEntries.Request emptyAppend = new AppendEntries.Request( leader, 1, -1, -1, RaftLogEntry.empty, 0 );
        Heartbeat heartbeat = new Heartbeat( null, 0, 0, 0 );

        batchHandler.handle( wrap( newEntry ) );
        batchHandler.handle( wrap( append ) );
        batchHandler.handle( wrap( heartbeat ) );
        batchHandler.handle( wrap( emptyAppend ) );
        verifyNoInteractions( downstreamHandler );

        // when
        batchHandler.run();
        batchHandler.run();
        batchHandler.run();
        batchHandler.run();

        // then
        InOrder inOrder = Mockito.inOrder( downstreamHandler );
        inOrder.verify( downstreamHandler ).handle( wrap( heartbeat ) );
        inOrder.verify( downstreamHandler ).handle( wrap( emptyAppend ) );
        inOrder.verify( downstreamHandler ).handle( wrap( append ) );
        inOrder.verify( downstreamHandler ).handle(
                wrap( new NewEntry.BatchRequest( singletonList( content( "" ) ) ) ) );
    }

    @Test
    void shouldDropMessagesAfterBeingStopped() throws Throwable
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, logProvider );

        NewEntry.Request message = new NewEntry.Request( null, null );
        batchHandler.stop();

        // when
        batchHandler.handle( wrap( message ) );
        batchHandler.run();

        // then
        verify( downstreamHandler, never() ).handle(
                ArgumentMatchers.any( InboundRaftMessageContainer.class ) );
        assertThat( logProvider ).forClass( BatchingMessageHandler.class ).forLevel( DEBUG )
                .containsMessageWithArguments( "This handler has been stopped, dropping the message: %s", wrap( message ) );
    }

    @Test
    void shouldGiveUpAddingMessagesInTheQueueIfTheHandlerHasBeenStopped() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler,
                                                                          new BoundedPriorityQueue.Config( 1, 1, 1024 ), BATCH_CONFIG, jobScheduler,
                                                                          NullLogProvider.getInstance() );
        NewEntry.Request message = new NewEntry.Request( null, new ReplicatedString( "dummy" ) );
        var inOrder = inOrder( jobScheduler );

        // when
        batchHandler.handle( wrap( message ) ); // add to queue

        // then - offered
        inOrder.verify( jobScheduler ).offerJob( any( Runnable.class ) );

        // when
        batchHandler.stop();
        batchHandler.handle( wrap( message ) );

        // then
        inOrder.verify( jobScheduler ).abort();
        // and - not offered because we are stopped
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldDelegateStart() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );
        RaftId raftId = RaftIdFactory.random();

        // when
        batchHandler.start( raftId );

        // then
        Mockito.verify( downstreamHandler ).start( raftId );
    }

    @Test
    void shouldDelegateStop() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        Mockito.verify( downstreamHandler ).stop();
    }

    @Test
    void shouldStopJob() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                                                                          BATCH_CONFIG, jobScheduler, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        Mockito.verify( jobScheduler ).abort();
    }

    private InboundRaftMessageContainer<?> wrap( RaftMessage message )
    {
        return wrap( now, message );
    }

    private InboundRaftMessageContainer<?> wrap( Instant instant, RaftMessage message )
    {
        return InboundRaftMessageContainer.of( instant, localRaftId, message );
    }

    private ReplicatedContent content( String content )
    {
        return new ReplicatedString( content );
    }

    private RaftLogEntry[] entries( long term, int min, int max )
    {
        RaftLogEntry[] entries = new RaftLogEntry[max - min + 1];
        for ( int i = min; i <= max; i++ )
        {
            entries[i - min] = new RaftLogEntry( term, new ReplicatedString( String.valueOf( i ) ) );
        }
        return entries;
    }
}
