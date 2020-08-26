/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v3.decoding;

import com.neo4j.causalclustering.catchup.Protocol;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.marshalling.ContentType;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.configuration.ServerGroupName;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.marshal.EndOfStreamException;

public final class RaftMessageDecoderV3 extends RaftMessageDecoder
{
    public RaftMessageDecoderV3( Protocol<ContentType> protocol )
    {
        super( protocol );
    }

    @Override
    protected Optional<LazyComposer> getLazyComposer( ReadableChannel channel, RaftMessages.Type messageType, RaftMemberId from )
            throws IOException, EndOfStreamException
    {
        return super.getLazyComposer( channel, messageType, from )
                    .or( () -> getLazyComposerForCurrentVersion( channel, messageType, from ) );
    }

    public static Optional<LazyComposer> getLazyComposerForCurrentVersion( ReadableChannel channel, RaftMessages.Type messageType, RaftMemberId from )
    {
        try
        {
            switch ( messageType )
            {
            case LEADERSHIP_TRANSFER_REQUEST:
            {
                return Optional.of( handleLeadershipTransferRequest( channel, from ) );
            }
            case LEADERSHIP_TRANSFER_REJECTION:
            {
                return Optional.of( handleLeadershipTransferRejection( channel, from ) );
            }
            default:
                return Optional.empty();
            }
        }
        catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    private static LazyComposer handleLeadershipTransferRejection( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long previousIndex = channel.getLong();
        long term = channel.getLong();

        return new SimpleMessageComposer( new RaftMessages.LeadershipTransfer.Rejection( from, previousIndex, term ) );
    }

    private static LazyComposer handleLeadershipTransferRequest( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long previousIndex = channel.getLong();
        long term = channel.getLong();
        int groupSize = channel.getInt();
        var groupStrings = new HashSet<String>();
        for ( var i = 0; i < groupSize; i++ )
        {
            groupStrings.add( StringMarshal.unmarshal( channel ) );
        }
        var groups = ServerGroupName.setOf( groupStrings );

        return new SimpleMessageComposer( new RaftMessages.LeadershipTransfer.Request( from, previousIndex, term, groups ) );
    }
}
