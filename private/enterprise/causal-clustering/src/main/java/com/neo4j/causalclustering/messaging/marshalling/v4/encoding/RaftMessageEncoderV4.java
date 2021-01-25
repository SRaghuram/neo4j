/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v4.encoding;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.UUIDMarshal;
import com.neo4j.causalclustering.messaging.marshalling.v3.encoding.RaftMessageEncoderV3;

public final class RaftMessageEncoderV4 extends RaftMessageEncoder
{

    @Override
    protected Handler createHandler( RaftMemberId.Marshal memberMarshal, NetworkWritableChannel channel )
    {
        return new HandlerV4( memberMarshal,
                              channel,
                              new RaftMessageEncoderV3.HandlerV3( memberMarshal, channel ) );
    }

    private static class HandlerV4 extends Handler
    {

        private final Handler handler;

        protected HandlerV4( RaftMemberId.Marshal memberMarshal,
                             NetworkWritableChannel channel,
                             Handler handler )
        {
            super( memberMarshal, channel );
            this.handler = handler;
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest ) throws Exception
        {
            return handler.handle( leadershipTransferRequest );
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection ) throws Exception
        {
            return handler.handle( leadershipTransferRejection );
        }

        @Override
        public Void handle( RaftMessages.StatusResponse statusResponse ) throws Exception
        {
            UUIDMarshal.INSTANCE.marshal( statusResponse.getRequestId(), channel );
            StringMarshal.marshal( channel, statusResponse.getStatus().message.name() );

            return null;
        }
    }
}
