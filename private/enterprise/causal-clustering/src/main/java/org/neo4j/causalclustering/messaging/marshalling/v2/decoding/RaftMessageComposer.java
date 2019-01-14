/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.time.Clock;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;

public class RaftMessageComposer extends MessageToMessageDecoder<Object>
{
    private final Queue<ReplicatedContent> replicatedContents = new LinkedList<>();
    private final Queue<Long> raftLogEntryTerms = new LinkedList<>();
    private RaftMessageDecoder.ClusterIdAwareMessageComposer messageComposer;
    private final Clock clock;

    public RaftMessageComposer( Clock clock )
    {
        this.clock = clock;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, Object msg, List<Object> out )
    {
        if ( msg instanceof ReplicatedContent )
        {
            replicatedContents.add( (ReplicatedContent) msg );
        }
        else if ( msg instanceof RaftLogEntryTermsDecoder.RaftLogEntryTerms )
        {
            for ( long term : ((RaftLogEntryTermsDecoder.RaftLogEntryTerms) msg).terms() )
            {
                raftLogEntryTerms.add( term );
            }
        }
        else if ( msg instanceof RaftMessageDecoder.ClusterIdAwareMessageComposer )
        {
            if ( messageComposer != null )
            {
                throw new IllegalStateException( "Received raft message header. Pipeline already contains message header waiting to build." );
            }
            messageComposer = (RaftMessageDecoder.ClusterIdAwareMessageComposer) msg;
        }
        else
        {
            throw new IllegalStateException( "Unexpected object in the pipeline: " + msg );
        }
        if ( messageComposer != null )
        {
            Optional<RaftMessages.ClusterIdAwareMessage> clusterIdAwareMessage = messageComposer.maybeCompose( clock, raftLogEntryTerms, replicatedContents );
            clusterIdAwareMessage.ifPresent( message ->
            {
                clear( message );
                out.add( message );
            } );
        }
    }

    private void clear( RaftMessages.ClusterIdAwareMessage message )
    {
        messageComposer = null;
        if ( !replicatedContents.isEmpty() || !raftLogEntryTerms.isEmpty() )
        {
            throw new IllegalStateException( String.format(
                    "Message [%s] was composed without using all resources in the pipeline. " +
                            "Pipeline still contains Replicated contents[%s] and RaftLogEntryTerms [%s]",
                    message, stringify( replicatedContents ), stringify( raftLogEntryTerms ) ) );
        }
    }

    private String stringify( Iterable<?> objects )
    {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<?> iterator = objects.iterator();
        while ( iterator.hasNext() )
        {
            stringBuilder.append( iterator.next() );
            if ( iterator.hasNext() )
            {
                stringBuilder.append( ", " );
            }
        }
        return stringBuilder.toString();
    }
}
