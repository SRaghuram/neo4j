/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v3.encoding;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;

public final class RaftMessageEncoderV3 extends RaftMessageEncoder
{
    @Override
    protected Handler createHandler( MemberId.Marshal memberMarshal, NetworkWritableChannel channel )
    {
        return new HandlerV3( memberMarshal, channel );
    }

    public static class HandlerV3 extends Handler
    {
        public HandlerV3( MemberId.Marshal memberMarshal, NetworkWritableChannel channel )
        {
            super( memberMarshal, channel );
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest ) throws Exception
        {
            channel.putLong( leadershipTransferRequest.previousIndex() );
            channel.putLong( leadershipTransferRequest.term() );
            var groups = leadershipTransferRequest.groups();
            channel.putInt( groups.size() );
            for ( var group : groups )
            {
                StringMarshal.marshal( channel, group.toString() );
            }
            return null;
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection ) throws Exception
        {
            channel.putLong( leadershipTransferRejection.previousIndex() );
            channel.putLong( leadershipTransferRejection.term() );
            return null;
        }

        @Override
        public Void handle( RaftMessages.StatusResponse statusResponse ) throws Exception
        {
            throw new UnsupportedOperationException( "Status response is not supported with Raft protocol v3!" );
        }
    }
}
