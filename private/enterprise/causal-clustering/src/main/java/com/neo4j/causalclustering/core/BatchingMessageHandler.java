/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.BoundedPriorityQueue.Removable;
import com.neo4j.causalclustering.core.consensus.ContinuousJob;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import com.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry;
import com.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantRaftIdAwareMessage;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.core.BoundedPriorityQueue.Result.OK;
import static java.lang.Long.max;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.helpers.ArrayUtil.lastOf;

/**
 * This class gets Raft messages as input and queues them up for processing. Some messages are
 * batched together before they are forwarded to the Raft machine, for reasons of efficiency.
 */
class BatchingMessageHandler implements Runnable, LifecycleMessageHandler<ReceivedInstantRaftIdAwareMessage<?>>
{
    public static class Config
    {
        private final int maxBatchCount;
        private final long maxBatchBytes;

        Config( int maxBatchCount, long maxBatchBytes )
        {
            this.maxBatchCount = maxBatchCount;
            this.maxBatchBytes = maxBatchBytes;
        }
    }

    private final LifecycleMessageHandler<ReceivedInstantRaftIdAwareMessage<?>> handler;
    private final Log log;
    private final BoundedPriorityQueue<ReceivedInstantRaftIdAwareMessage<?>> inQueue;
    private final ContinuousJob job;
    private final List<ReplicatedContent> contentBatch; // reused for efficiency
    private final List<RaftLogEntry> entryBatch; // reused for efficiency
    private final Config batchConfig;

    private volatile boolean stopped;
    private volatile BoundedPriorityQueue.Result lastResult = OK;
    private AtomicLong droppedCount = new AtomicLong();

    BatchingMessageHandler( LifecycleMessageHandler<ReceivedInstantRaftIdAwareMessage<?>> handler,
            BoundedPriorityQueue.Config inQueueConfig, Config batchConfig, Function<Runnable,ContinuousJob> jobFactory,
            LogProvider logProvider )
    {
        this.handler = handler;
        this.log = logProvider.getLog( getClass() );
        this.batchConfig = batchConfig;
        this.contentBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.entryBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.inQueue = new BoundedPriorityQueue<>( inQueueConfig, ContentSize::of, new MessagePriority() );
        this.job = jobFactory.apply( this );
    }

    static ComposableMessageHandler composable( BoundedPriorityQueue.Config inQueueConfig, Config batchConfig,
            Function<Runnable,ContinuousJob> jobSchedulerFactory, LogProvider logProvider )
    {
        return delegate -> new BatchingMessageHandler( delegate, inQueueConfig, batchConfig, jobSchedulerFactory, logProvider );
    }

    @Override
    public void start( RaftId raftId ) throws Exception
    {
        handler.start( raftId );
        job.start();
    }

    @Override
    public void stop() throws Exception
    {
        stopped = true;
        handler.stop();
        job.stop();
    }

    @Override
    public void handle( ReceivedInstantRaftIdAwareMessage<?> message )
    {
        if ( stopped )
        {
            log.debug( "This handler has been stopped, dropping the message: %s", message );
            return;
        }

        BoundedPriorityQueue.Result result = inQueue.offer( message );
        logQueueState( result );
    }

    private void logQueueState( BoundedPriorityQueue.Result result )
    {
        if ( result != OK )
        {
            droppedCount.incrementAndGet();
        }

        if ( result != lastResult )
        {
            if ( result == OK )
            {
                log.info( "Raft in-queue not dropping messages anymore. Dropped %d messages.",
                        droppedCount.getAndSet( 0 ) );
            }
            else
            {
                log.warn( "Raft in-queue dropping messages after: " + result );
            }
            lastResult = result;
        }
    }

    @Override
    public void run()
    {
        Optional<ReceivedInstantRaftIdAwareMessage<?>> baseMessage;
        try
        {
            baseMessage = inQueue.poll( 1, SECONDS );
        }
        catch ( InterruptedException e )
        {
            log.warn( "Not expecting to be interrupted.", e );
            return;
        }

        if ( !baseMessage.isPresent() )
        {
            return;
        }

        ReceivedInstantRaftIdAwareMessage batchedMessage = baseMessage.get().message().dispatch(
                new BatchingHandler( baseMessage.get() ) );

        handler.handle( batchedMessage == null ? baseMessage.get() : batchedMessage );
    }

    /**
     * Batches together the content of NewEntry.Requests for efficient handling.
     */
    private NewEntry.BatchRequest batchNewEntries( NewEntry.Request first )
    {
        contentBatch.clear();

        contentBatch.add( first.content() );
        long totalBytes = first.content().size().orElse( 0L );

        while ( contentBatch.size() < batchConfig.maxBatchCount )
        {
            Optional<Removable<NewEntry.Request>> peeked = peekNext( NewEntry.Request.class );

            if ( !peeked.isPresent() )
            {
                break;
            }

            ReplicatedContent content = peeked.get().get().content();

            if ( content.size().isPresent() && (totalBytes + content.size().getAsLong()) > batchConfig.maxBatchBytes )
            {
                break;
            }

            contentBatch.add( content );

            boolean removed = peeked.get().remove();
            assert removed; // single consumer assumed
        }

        /*
         * Individual NewEntry.Requests are batched together into a BatchRequest to take advantage
         * of group commit into the Raft log and any other batching benefits.
         */
        return new NewEntry.BatchRequest( contentBatch );
    }

    private AppendEntries.Request batchAppendEntries( AppendEntries.Request first )
    {
        entryBatch.clear();

        long totalBytes = 0;

        for ( RaftLogEntry entry : first.entries() )
        {
            totalBytes += entry.content().size().orElse( 0L );
            entryBatch.add( entry );
        }

        long leaderCommit = first.leaderCommit();
        long lastTerm = lastOf( first.entries() ).term();

        while ( entryBatch.size() < batchConfig.maxBatchCount )
        {
            Optional<Removable<AppendEntries.Request>> peeked = peekNext( AppendEntries.Request.class );

            if ( !peeked.isPresent() )
            {
                break;
            }

            AppendEntries.Request request = peeked.get().get();

            if ( request.entries().length == 0 || !consecutiveOrigin( first, request, entryBatch.size() ) )
            {
                // probe (RaftLogShipper#sendEmpty) or leader switch
                break;
            }

            assert lastTerm == request.prevLogTerm();

            // note that this code is backwards compatible, but AppendEntries.Request generation by the leader
            // will be changed to only generate single entry AppendEntries.Requests and the code here
            // will be responsible for the batching of the individual and consecutive entries

            RaftLogEntry[] entries = request.entries();
            lastTerm = lastOf( entries ).term();

            if ( entries.length + entryBatch.size() > batchConfig.maxBatchCount )
            {
                break;
            }

            long requestBytes = Arrays.stream( entries )
                    .mapToLong( entry -> entry.content().size().orElse( 0L ) )
                    .sum();

            if ( requestBytes > 0 && (totalBytes + requestBytes) > batchConfig.maxBatchBytes )
            {
                break;
            }

            entryBatch.addAll( Arrays.asList( entries ) );
            totalBytes += requestBytes;
            leaderCommit = max( leaderCommit, request.leaderCommit() );

            boolean removed = peeked.get().remove();
            assert removed; // single consumer assumed
        }

        return new AppendEntries.Request( first.from(), first.leaderTerm(), first.prevLogIndex(), first.prevLogTerm(),
                entryBatch.toArray( RaftLogEntry.empty ), leaderCommit );
    }

    private boolean consecutiveOrigin( AppendEntries.Request first, AppendEntries.Request request, int currentSize )
    {
        if ( request.leaderTerm() != first.leaderTerm() )
        {
            return false;
        }
        else
        {
            return request.prevLogIndex() == first.prevLogIndex() + currentSize;
        }
    }

    private <M> Optional<Removable<M>> peekNext( Class<M> acceptedType )
    {
        return inQueue.peek()
                .filter( r -> acceptedType.isInstance( r.get().message() ) )
                .map( r -> r.map( m -> acceptedType.cast( m.message() ) ) );
    }

    private static class ContentSize extends RaftMessages.HandlerAdaptor<Long,RuntimeException>
    {
        private static final ContentSize INSTANCE = new ContentSize();

        private ContentSize()
        {
        }

        static long of( ReceivedInstantRaftIdAwareMessage<?> message )
        {
            Long dispatch = message.message().dispatch( INSTANCE );
            return dispatch == null ? 0L : dispatch;
        }

        @Override
        public Long handle( NewEntry.Request request ) throws RuntimeException
        {
            return request.content().size().orElse( 0L );
        }

        @Override
        public Long handle( AppendEntries.Request request ) throws RuntimeException
        {
            long totalSize = 0L;
            for ( RaftLogEntry entry : request.entries() )
            {
                if ( entry.content().size().isPresent() )
                {
                    totalSize += entry.content().size().getAsLong();
                }
            }
            return totalSize;
        }
    }

    private class MessagePriority extends RaftMessages.HandlerAdaptor<Integer,RuntimeException>
            implements Comparator<ReceivedInstantRaftIdAwareMessage<?>>
    {
        private final Integer BASE_PRIORITY = 10; // lower number means higher priority

        @Override
        public Integer handle( AppendEntries.Request request )
        {

            // 0 length means this is a heartbeat, so let it be handled with higher priority
            return request.entries().length == 0 ? BASE_PRIORITY : 20;
        }

        @Override
        public Integer handle( NewEntry.Request request )
        {
            return 30;
        }

        @Override
        public int compare( ReceivedInstantRaftIdAwareMessage<?> messageA,
                ReceivedInstantRaftIdAwareMessage<?> messageB )
        {
            int priorityA = getPriority( messageA );
            int priorityB = getPriority( messageB );

            return Integer.compare( priorityA, priorityB );
        }

        private int getPriority( ReceivedInstantRaftIdAwareMessage<?> message )
        {
            Integer priority = message.message().dispatch( this );
            return priority == null ? BASE_PRIORITY : priority;
        }
    }

    private class BatchingHandler extends RaftMessages.HandlerAdaptor<ReceivedInstantRaftIdAwareMessage,RuntimeException>
    {
        private final ReceivedInstantRaftIdAwareMessage<?> baseMessage;

        BatchingHandler( ReceivedInstantRaftIdAwareMessage<?> baseMessage )
        {
            this.baseMessage = baseMessage;
        }

        @Override
        public ReceivedInstantRaftIdAwareMessage handle( NewEntry.Request request ) throws RuntimeException
        {
            NewEntry.BatchRequest newEntryBatch = batchNewEntries( request );
            return ReceivedInstantRaftIdAwareMessage.of( baseMessage.receivedAt(), baseMessage.raftId(), newEntryBatch );
        }

        @Override
        public ReceivedInstantRaftIdAwareMessage handle( AppendEntries.Request request ) throws
                RuntimeException
        {
            if ( request.entries().length == 0 )
            {
                // this is a heartbeat, so let it be solo handled
                return null;
            }

            AppendEntries.Request appendEntriesBatch = batchAppendEntries( request );
            return ReceivedInstantRaftIdAwareMessage.of( baseMessage.receivedAt(), baseMessage.raftId(), appendEntriesBatch );
        }
    }
}
