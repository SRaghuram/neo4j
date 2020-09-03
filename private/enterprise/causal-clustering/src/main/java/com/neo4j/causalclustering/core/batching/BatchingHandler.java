/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.RaftGroupId;
import org.apache.commons.lang3.mutable.MutableInt;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static java.lang.Long.max;
import static java.util.Arrays.stream;
import static org.neo4j.internal.helpers.ArrayUtil.lastOf;

class BatchingHandler extends RaftMessages.HandlerAdaptor<RaftMessages.InboundRaftMessageContainer<?>,RuntimeException>
{
    private final BatchingConfig batchConfig;
    private final BoundedPriorityQueue<RaftMessages.InboundRaftMessageContainer<?>> inQueue;
    private final Instant receivedAt;
    private final RaftGroupId raftGroupId;
    private final List<ReplicatedContent> contentBatch;
    private final List<RaftLogEntry> entryBatch;

    BatchingHandler( Instant receivedAt, RaftGroupId raftGroupId, BatchingConfig batchConfig,
            BoundedPriorityQueue<RaftMessages.InboundRaftMessageContainer<?>> inQueue,
            List<RaftLogEntry> entryBatch, List<ReplicatedContent> contentBatch )
    {
        this.receivedAt = receivedAt;
        this.raftGroupId = raftGroupId;
        this.contentBatch = contentBatch;
        this.entryBatch = entryBatch;
        this.batchConfig = batchConfig;
        this.inQueue = inQueue;
    }

    @Override
    public RaftMessages.InboundRaftMessageContainer<?> handle( RaftMessages.NewEntry.Request request ) throws RuntimeException
    {
        RaftMessages.NewEntry.BatchRequest newEntryBatch = batchNewEntries( request );
        return RaftMessages.InboundRaftMessageContainer.of( receivedAt, raftGroupId, newEntryBatch );
    }

    @Override
    public RaftMessages.InboundRaftMessageContainer<?> handle( RaftMessages.AppendEntries.Request request ) throws RuntimeException
    {
        if ( request.entries().length == 0 )
        {
            // this is a heartbeat, so let it be solo handled
            return null;
        }

        var appendEntriesBatch = batchAppendEntries( request );
        return RaftMessages.InboundRaftMessageContainer.of( receivedAt, raftGroupId, appendEntriesBatch );
    }

    /**
     * Batches together the content of NewEntry.Requests for efficient handling.
     */
    private RaftMessages.NewEntry.BatchRequest batchNewEntries( RaftMessages.NewEntry.Request first )
    {
        contentBatch.add( first.content() );
        var totalSize = new MutableInt( getSize( first.content() ) );

        while ( contentBatch.size() < batchConfig.maxBatchCount )
        {
            var optionalRequest =
                    pollNext( RaftMessages.NewEntry.Request.class,
                              request -> totalSize.addAndGet( getSize( request.content() ) ) <= batchConfig.maxBatchBytes );
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
        return new RaftMessages.NewEntry.BatchRequest( contentBatch );
    }

    private RaftMessages.AppendEntries.Request batchAppendEntries( RaftMessages.AppendEntries.Request first )
    {
        long totalBytes = addAndGetSize( first.entries(), entryBatch );
        long leaderCommit = first.leaderCommit();
        long lastTerm = lastOf( first.entries() ).term();

        while ( entryBatch.size() < batchConfig.maxBatchCount )
        {
            var optionalRequest = pollNext( RaftMessages.AppendEntries.Request.class,
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
            totalBytes += addAndGetSize( entries, entryBatch );
            leaderCommit = max( leaderCommit, request.leaderCommit() );
            lastTerm = lastOf( entries ).term();
        }

        return new RaftMessages.AppendEntries.Request( first.from(), first.leaderTerm(), first.prevLogIndex(), first.prevLogTerm(), entryBatch
                .toArray( RaftLogEntry.empty ),
                                                       leaderCommit );
    }

    private <M> Optional<M> pollNext( Class<M> acceptedType, Predicate<M> additionalPredicate )
    {
        return inQueue.pollIf( typeSafePoll( acceptedType, additionalPredicate ) ).map( r -> acceptedType.cast( r.message() ) );
    }

    private <M> Predicate<RaftMessages.InboundRaftMessageContainer<?>> typeSafePoll( Class<M> instanceType, Predicate<M> additionalPredicate )
    {
        return InboundRaftMessageContainer -> instanceType.isInstance( InboundRaftMessageContainer.message() ) &&
                                                    additionalPredicate.test( instanceType.cast( InboundRaftMessageContainer.message() ) );
    }

    private static long addAndGetSize( RaftLogEntry[] entries, List<RaftLogEntry> entryBatch )
    {
        long totalBytes = 0;
        for ( RaftLogEntry entry : entries )
        {
            totalBytes += getSize( entry.content() );
            entryBatch.add( entry );
        }
        return totalBytes;
    }

    private static Predicate<RaftMessages.AppendEntries.Request> validAppendEntriesPoll( RaftMessages.AppendEntries.Request first, long currentBytes,
            int currentSize, int maxSize,
            long maxBytes )
    {
        Predicate<RaftMessages.AppendEntries.Request> consecutiveOrigin =
                request -> request.entries().length != 0 && consecutiveOrigin( first, request, currentSize );
        Predicate<RaftMessages.AppendEntries.Request> checkLength =
                request -> request.entries().length + currentSize <= maxSize;
        Predicate<RaftMessages.AppendEntries.Request> checkSize =
                request -> {
                    long requestBytes = getSize( request.entries() );
                    return !(requestBytes > 0 && (currentBytes + requestBytes) > maxBytes);
                };

        return consecutiveOrigin.and( checkLength ).and( checkSize );
    }

    private static boolean consecutiveOrigin( RaftMessages.AppendEntries.Request first, RaftMessages.AppendEntries.Request request, int currentSize )
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

    private static long getSize( RaftLogEntry[] entries )
    {
        return stream( entries ).flatMapToLong( raftLogEntry -> raftLogEntry.content().size().stream() ).sum();
    }

    private static long getSize( ReplicatedContent content )
    {
        return content.size().orElse( 0L );
    }
}
