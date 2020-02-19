/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import com.neo4j.causalclustering.core.consensus.RaftMessages.InboundRaftMessageContainer;
import com.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.helper.scheduling.QueueingScheduler;
import com.neo4j.causalclustering.helper.scheduling.ReoccurringJobQueue;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.ComposableMessageHandler;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.core.BoundedPriorityQueue.Result.OK;
import static java.lang.Long.max;
import static java.util.Arrays.stream;
import static org.neo4j.internal.helpers.ArrayUtil.lastOf;

/**
 * This class gets Raft messages as input and queues them up for processing. Some messages are
 * batched together before they are forwarded to the Raft machine, for reasons of efficiency.
 */
class BatchingMessageHandler implements Runnable, LifecycleMessageHandler<InboundRaftMessageContainer<?>>
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

    private final LifecycleMessageHandler<InboundRaftMessageContainer<?>> handler;
    private final Log log;
    private final BoundedPriorityQueue<InboundRaftMessageContainer<?>> inQueue;
    private final QueueingScheduler scheduler;
    private final List<ReplicatedContent> contentBatch; // reused for efficiency
    private final List<RaftLogEntry> entryBatch; // reused for efficiency
    private final Config batchConfig;

    private volatile boolean stopped;
    private volatile BoundedPriorityQueue.Result lastResult = OK;
    private AtomicLong droppedCount = new AtomicLong();

    BatchingMessageHandler( LifecycleMessageHandler<InboundRaftMessageContainer<?>> handler,
                            BoundedPriorityQueue.Config inQueueConfig, Config batchConfig, QueueingScheduler scheduler,
                            LogProvider logProvider )
    {
        this.handler = handler;
        this.log = logProvider.getLog( getClass() );
        this.batchConfig = batchConfig;
        this.contentBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.entryBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.inQueue = new BoundedPriorityQueue<>( inQueueConfig, ContentSize::of, new MessagePriority() );
        this.scheduler = scheduler;
    }

    static ComposableMessageHandler composable( BoundedPriorityQueue.Config inQueueConfig, Config batchConfig,
                                                JobScheduler jobScheduler, LogProvider logProvider )
    {
        return delegate -> new BatchingMessageHandler( delegate, inQueueConfig, batchConfig,
                                                       new QueueingScheduler( jobScheduler, Group.RAFT_BATCH_HANDLER,
                                                                              logProvider.getLog( BatchingMessageHandler.class ),
                                                                              1, new ReoccurringJobQueue<>() ), logProvider );
    }

    @Override
    public void start( RaftId raftId ) throws Exception
    {
        handler.start( raftId );
        scheduler.offerJob( this );
    }

    @Override
    public void stop() throws Exception
    {
        stopped = true;
        handler.stop();
        scheduler.abort();
    }

    @Override
    public void handle( InboundRaftMessageContainer<?> message )
    {
        if ( stopped )
        {
            log.debug( "This handler has been stopped, dropping the message: %s", message );
            return;
        }

        BoundedPriorityQueue.Result result = inQueue.offer( message );
        logQueueState( result );
        if ( result == OK )
        {
            scheduler.offerJob( this );
        }
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
        inQueue.poll().ifPresent( message ->
                                  {
                                      var batchedMessage = message.message().dispatch( new BatchingHandler( message ) );
                                      handler.handle( batchedMessage == null ? message : batchedMessage );
                                  } );
    }

    /**
     * Batches together the content of NewEntry.Requests for efficient handling.
     */
    private NewEntry.BatchRequest batchNewEntries( NewEntry.Request first )
    {
        contentBatch.clear();

        contentBatch.add( first.content() );
        long totalBytes = getSize( first.content() );

        while ( contentBatch.size() < batchConfig.maxBatchCount )
        {
            var optionalRequest = pollNext( NewEntry.Request.class, request -> (totalBytes + getSize( request.content() )) <= batchConfig.maxBatchBytes );
            if ( optionalRequest.isEmpty() )
            {
                break;
            }
            contentBatch.add( optionalRequest.get().content() );
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

        long totalBytes = addAndGetSize( first.entries() );
        long leaderCommit = first.leaderCommit();
        long lastTerm = lastOf( first.entries() ).term();

        while ( entryBatch.size() < batchConfig.maxBatchCount )
        {
            var optionalRequest = pollNext( AppendEntries.Request.class,
                                            validAppendEntriesPoll( first, totalBytes, entryBatch.size(), batchConfig.maxBatchCount,
                                                                    batchConfig.maxBatchBytes ) );
            if ( optionalRequest.isEmpty() )
            {
                break;
            }
            var request = optionalRequest.get();

            assert lastTerm == request.prevLogTerm();

            // note that this code is backwards compatible, but AppendEntries.Request generation by the leader
            // will be changed to only generate single entry AppendEntries.Requests and the code here
            // will be responsible for the batching of the individual and consecutive entries
            var entries = request.entries();
            totalBytes += addAndGetSize( entries );
            leaderCommit = max( leaderCommit, request.leaderCommit() );
            lastTerm = lastOf( entries ).term();
        }

        return new AppendEntries.Request( first.from(), first.leaderTerm(), first.prevLogIndex(), first.prevLogTerm(), entryBatch.toArray( RaftLogEntry.empty ),
                                          leaderCommit );
    }

    private static Predicate<AppendEntries.Request> validAppendEntriesPoll( AppendEntries.Request first, long currentBytes, int currentSize, int maxSize,
            long maxBytes )
    {
        Predicate<AppendEntries.Request> consecutiveOrigin =
                request -> request.entries().length != 0 && consecutiveOrigin( first, request, currentSize );
        Predicate<AppendEntries.Request> checkLength =
                request -> request.entries().length + currentSize <= maxSize;
        Predicate<AppendEntries.Request> checkSize =
                request -> {
                    long requestBytes = getSize( request.entries() );
                    return !(requestBytes > 0 && (currentBytes + requestBytes) > maxBytes);
                };

        return consecutiveOrigin.and( checkLength ).and( checkSize );
    }

    private static long getSize( ReplicatedContent content )
    {
        return content.size().orElse( 0L );
    }

    private static long getSize( RaftLogEntry[] entries )
    {
        return stream( entries ).flatMapToLong( raftLogEntry -> raftLogEntry.content().size().stream() ).sum();
    }

    private static boolean consecutiveOrigin( AppendEntries.Request first, AppendEntries.Request request, int currentSize )
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

    private long addAndGetSize( RaftLogEntry[] entries )
    {
        long totalBytes = 0;
        for ( RaftLogEntry entry : entries )
        {
            totalBytes += getSize( entry.content() );
            entryBatch.add( entry );
        }
        return totalBytes;
    }

    private <M> Optional<M> pollNext( Class<M> acceptedType, Predicate<M> additionalPredicate )
    {
        return inQueue.pollIf( typeSafePoll( acceptedType, additionalPredicate ) ).map( r -> acceptedType.cast( r.message() ) );
    }

    private <M> Predicate<ReceivedInstantRaftIdAwareMessage<?>> typeSafePoll( Class<M> instanceType, Predicate<M> additionalPredicate )
    {
        return receivedInstantRaftIdAwareMessage -> instanceType.isInstance( receivedInstantRaftIdAwareMessage.message() ) &&
                                                    additionalPredicate.test( instanceType.cast( receivedInstantRaftIdAwareMessage.message() ) );
    }

    private static class ContentSize extends RaftMessages.HandlerAdaptor<Long,RuntimeException>
    {

        private static final ContentSize INSTANCE = new ContentSize();

        private ContentSize()
        {
        }

        static long of( InboundRaftMessageContainer<?> messageContainer )
        {
            Long dispatch = messageContainer.message().dispatch( INSTANCE );
            return dispatch == null ? 0L : dispatch;
        }

        @Override
        public Long handle( NewEntry.Request request ) throws RuntimeException
        {
            return getSize( request.content() );
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

    private static class MessagePriority extends RaftMessages.HandlerAdaptor<Integer,RuntimeException>
            implements Comparator<InboundRaftMessageContainer<?>>
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
        public int compare( InboundRaftMessageContainer<?> messageA,
                InboundRaftMessageContainer<?> messageB )
        {
            int priorityA = getPriority( messageA );
            int priorityB = getPriority( messageB );

            return Integer.compare( priorityA, priorityB );
        }

        private int getPriority( InboundRaftMessageContainer<?> message )
        {
            Integer priority = message.message().dispatch( this );
            return priority == null ? BASE_PRIORITY : priority;
        }
    }
    private class BatchingHandler extends RaftMessages.HandlerAdaptor<InboundRaftMessageContainer,RuntimeException>
    {

        private final InboundRaftMessageContainer<?> baseMessage;
        BatchingHandler( InboundRaftMessageContainer<?> baseMessage )
        {
            this.baseMessage = baseMessage;
        }

        @Override
        public InboundRaftMessageContainer handle( NewEntry.Request request ) throws RuntimeException
        {
            NewEntry.BatchRequest newEntryBatch = batchNewEntries( request );
            return InboundRaftMessageContainer.of( baseMessage.receivedAt(), baseMessage.raftId(), newEntryBatch );
        }

        @Override
        public InboundRaftMessageContainer handle( AppendEntries.Request request ) throws
                RuntimeException
        {
            if ( request.entries().length == 0 )
            {
                // this is a heartbeat, so let it be solo handled
                return null;
            }

            AppendEntries.Request appendEntriesBatch = batchAppendEntries( request );
            return InboundRaftMessageContainer.of( baseMessage.receivedAt(), baseMessage.raftId(), appendEntriesBatch );
        }
    }
}

