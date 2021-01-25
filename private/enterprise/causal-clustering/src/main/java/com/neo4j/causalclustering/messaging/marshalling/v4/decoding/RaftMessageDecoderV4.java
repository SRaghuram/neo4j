/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v4.decoding;

import com.neo4j.causalclustering.catchup.Protocol;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.state.machines.status.Status;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.marshalling.ContentType;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.UUIDMarshal;
import com.neo4j.causalclustering.messaging.marshalling.v3.decoding.RaftMessageDecoderV3;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.marshal.EndOfStreamException;

public class RaftMessageDecoderV4 extends RaftMessageDecoder
{
    public RaftMessageDecoderV4( Protocol<ContentType> protocol )
    {
        super( protocol );
    }

    @Override
    protected Optional<LazyComposer> getLazyComposer( ReadableChannel channel, RaftMessages.Type messageType, RaftMemberId from )
            throws IOException, EndOfStreamException
    {
        return super.getLazyComposer( channel, messageType, from )
                    .or( () -> RaftMessageDecoderV3.getLazyComposerForCurrentVersion( channel, messageType, from ) )
                    .or( () -> getLazyComposerForCurrentVersion( channel, messageType, from ) );
    }

    public Optional<LazyComposer> getLazyComposerForCurrentVersion( ReadableChannel channel, RaftMessages.Type messageType, RaftMemberId from )
    {
        try
        {
            switch ( messageType )
            {
            case STATUS_RESPONSE:
            {
                return Optional.of( handleStatusResponse( channel, from ));
            }
            default:
                return Optional.empty();
            }
        }
        catch ( IOException | EndOfStreamException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    private LazyComposer handleStatusResponse( ReadableChannel channel, RaftMemberId from ) throws IOException, EndOfStreamException
    {
        var messageId = UUIDMarshal.INSTANCE.unmarshal( channel );
        String statusMessage = StringMarshal.unmarshal( channel );
        return new SimpleMessageComposer(
                new RaftMessages.StatusResponse( from, new Status( Status.Message.valueOf( statusMessage ) ), messageId ) );
    }
}
