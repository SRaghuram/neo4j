/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;

import static com.neo4j.causalclustering.core.batching.Helpers.contentWithOneByte;
import static com.neo4j.causalclustering.core.batching.Helpers.emptyContent;
import static com.neo4j.causalclustering.core.batching.Helpers.message;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchingHandlerTest
{
    static BatchingConfig[] configs()
    {
        return new BatchingConfig[]{new BatchingConfig( 0, 0 ),
                                    new BatchingConfig( 1, 1 ),
                                    new BatchingConfig( 10, 8 )};
    }

    @ParameterizedTest
    @MethodSource( "configs" )
    void shouldRespectCountOnNewEntry( BatchingConfig config )
    {
        var message = message( emptyContent() );
        var queue = createFIFOPriorityQueue();
        // fill queue
        for ( int i = 0; i < config.maxBatchCount; i++ )
        {
            queue.offer( message );
        }
        var batchingHandler = new BatchingHandler( message.receivedAt(), message.raftId(), config, queue, new ArrayList<>(), new ArrayList<>() );

        var batchedMessage =
                (RaftMessages.InboundRaftMessageContainer<RaftMessages.NewEntry.BatchRequest>) message.message().dispatch( batchingHandler );

        int expected = config.maxBatchCount == 0 ? 1 : config.maxBatchCount;

        assertEquals( expected, batchedMessage.message().contents().size() );
    }

    @ParameterizedTest
    @MethodSource( "configs" )
    void shouldRespectSizeOnNewEntry( BatchingConfig config )
    {
        var message = message( Helpers.contentWithOneByte() );
        var queue = createFIFOPriorityQueue();
        // fill queue
        for ( int i = 0; i < config.maxBatchCount; i++ )
        {
            queue.offer( message );
        }
        var batchingHandler = new BatchingHandler( message.receivedAt(), message.raftId(), config, queue, new ArrayList<>(), new ArrayList<>() );

        var batchedMessage =
                (RaftMessages.InboundRaftMessageContainer<RaftMessages.NewEntry.BatchRequest>) message.message().dispatch( batchingHandler );

        int expected = config.maxBatchBytes == 0 ? 1 : (int) config.maxBatchBytes;

        assertEquals( expected, batchedMessage.message().contents().size() );
    }

    @ParameterizedTest
    @MethodSource( "configs" )
    void shouldRespectCountOnAppendEntry( BatchingConfig config )
    {
        var message = message( 0, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} );
        var queue = createFIFOPriorityQueue();
        // fill queue with consecutive messages
        for ( int i = 1; i <= config.maxBatchCount; i++ )
        {
            queue.offer( message( i, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} ) );
        }
        var batchingHandler = new BatchingHandler( message.receivedAt(), message.raftId(), config, queue, new ArrayList<>(), new ArrayList<>() );

        var batchedMessage =
                (RaftMessages.InboundRaftMessageContainer<RaftMessages.AppendEntries.Request>) message.message().dispatch( batchingHandler );

        int expected = config.maxBatchCount == 0 ? 1 : config.maxBatchCount;

        assertEquals( expected, batchedMessage.message().entries().length );
    }

    @ParameterizedTest
    @MethodSource( "configs" )
    void shouldRespectSizeOnAppendEntries( BatchingConfig config )
    {
        var message = message( 0, new RaftLogEntry[]{new RaftLogEntry( 1, contentWithOneByte() )} );
        var queue = createFIFOPriorityQueue();
        // fill queue
        for ( int i = 1; i <= config.maxBatchCount; i++ )
        {
            queue.offer( message( i, new RaftLogEntry[]{new RaftLogEntry( 1, contentWithOneByte() )} ) );
        }
        var batchingHandler = new BatchingHandler( message.receivedAt(), message.raftId(), config, queue, new ArrayList<>(), new ArrayList<>() );

        var batchedMessage =
                (RaftMessages.InboundRaftMessageContainer<RaftMessages.AppendEntries.Request>) message.message().dispatch( batchingHandler );

        int expected = config.maxBatchBytes == 0 ? 1 : (int) config.maxBatchBytes;

        assertEquals( expected, batchedMessage.message().entries().length );
    }

    @Test
    void shouldStopWhenNotConsecutiveLogIndex()
    {
        var message = message( 0, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} );
        var queue = createFIFOPriorityQueue();
        queue.offer( message( 1, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} ) );
        queue.offer( message( 2, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} ) );
        // not consecutive
        queue.offer( message( 4, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} ) );

        var batchingHandler =
                new BatchingHandler( message.receivedAt(), message.raftId(), new BatchingConfig( 100, 100 ), queue, new ArrayList<>(), new ArrayList<>() );

        var batchedMessage =
                (RaftMessages.InboundRaftMessageContainer<RaftMessages.AppendEntries.Request>) message.message().dispatch( batchingHandler );

        int expected = 3; // three consecutive messages

        assertEquals( expected, batchedMessage.message().entries().length );
    }

    @Test
    void shouldStopWhenNotConsecutiveType()
    {
        var message = message( emptyContent() );
        var queue = createFIFOPriorityQueue();
        queue.offer( message );
        // different type
        queue.offer( message( 1, new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} ) );
        queue.offer( message );

        var batchingHandler =
                new BatchingHandler( message.receivedAt(), message.raftId(), new BatchingConfig( 100, 100 ), queue, new ArrayList<>(), new ArrayList<>() );

        var batchedMessage =
                (RaftMessages.InboundRaftMessageContainer<RaftMessages.NewEntry.BatchRequest>) message.message().dispatch( batchingHandler );

        int expected = 2; // three consecutive messages

        assertEquals( expected, batchedMessage.message().contents().size() );
    }

    private BoundedPriorityQueue<RaftMessages.InboundRaftMessageContainer<?>> createFIFOPriorityQueue()
    {
        return new BoundedPriorityQueue<>( new BoundedPriorityQueue.Config( 100, 1000 ), ContentSizeHandler::of,
                                           ( o1, o2 ) -> 0 );
    }
}
