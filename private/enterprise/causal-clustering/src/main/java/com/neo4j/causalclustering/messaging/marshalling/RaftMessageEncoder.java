/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RaftMessageEncoder extends MessageToByteEncoder<RaftMessages.OutboundRaftMessageContainer>
{
    @Override
    public void encode( ChannelHandlerContext ctx, RaftMessages.OutboundRaftMessageContainer decoratedMessage, ByteBuf out ) throws Exception
    {
        RaftMessages.RaftMessage message = decoratedMessage.message();
        RaftGroupId raftGroupId = decoratedMessage.raftGroupId();
        RaftMemberId.Marshal memberMarshal = RaftMemberId.Marshal.INSTANCE;

        NetworkWritableChannel channel = new NetworkWritableChannel( out );
        channel.put( ContentType.Message.get() );
        RaftGroupId.Marshal.INSTANCE.marshal( raftGroupId, channel );
        channel.putInt( message.type().ordinal() );
        memberMarshal.marshal( message.from(), channel );

        message.dispatch( createHandler( memberMarshal, channel ) );
    }

    protected Handler createHandler( RaftMemberId.Marshal memberMarshal, NetworkWritableChannel channel )
    {
        return new Handler( memberMarshal, channel );
    }

    protected static class Handler implements RaftMessages.Handler<Void,Exception>
    {
        protected final RaftMemberId.Marshal memberMarshal;
        protected final NetworkWritableChannel channel;

        protected Handler( RaftMemberId.Marshal memberMarshal, NetworkWritableChannel channel )
        {
            this.memberMarshal = memberMarshal;
            this.channel = channel;
        }

        @Override
        public Void handle( RaftMessages.Vote.Request voteRequest ) throws Exception
        {
            memberMarshal.marshal( voteRequest.candidate(), channel );
            channel.putLong( voteRequest.term() );
            channel.putLong( voteRequest.lastLogIndex() );
            channel.putLong( voteRequest.lastLogTerm() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.Vote.Response voteResponse )
        {
            channel.putLong( voteResponse.term() );
            channel.put( (byte) (voteResponse.voteGranted() ? 1 : 0) );

            return null;
        }

        @Override
        public Void handle( RaftMessages.PreVote.Request preVoteRequest ) throws Exception
        {
            memberMarshal.marshal( preVoteRequest.candidate(), channel );
            channel.putLong( preVoteRequest.term() );
            channel.putLong( preVoteRequest.lastLogIndex() );
            channel.putLong( preVoteRequest.lastLogTerm() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.PreVote.Response preVoteResponse )
        {
            channel.putLong( preVoteResponse.term() );
            channel.put( (byte) (preVoteResponse.voteGranted() ? 1 : 0) );

            return null;
        }

        @Override
        public Void handle( RaftMessages.AppendEntries.Request appendRequest ) throws Exception
        {
            channel.putLong( appendRequest.leaderTerm() );
            channel.putLong( appendRequest.prevLogIndex() );
            channel.putLong( appendRequest.prevLogTerm() );
            channel.putLong( appendRequest.leaderCommit() );
            channel.putInt( appendRequest.entries().length );

            return null;
        }

        @Override
        public Void handle( RaftMessages.AppendEntries.Response appendResponse )
        {
            channel.putLong( appendResponse.term() );
            channel.put( (byte) (appendResponse.success() ? 1 : 0) );
            channel.putLong( appendResponse.matchIndex() );
            channel.putLong( appendResponse.appendIndex() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.NewEntry.Request newEntryRequest ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.Heartbeat heartbeat )
        {
            channel.putLong( heartbeat.leaderTerm() );
            channel.putLong( heartbeat.commitIndexTerm() );
            channel.putLong( heartbeat.commitIndex() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            // Heartbeat Response does not have any data attached to it.
            return null;
        }

        @Override
        public Void handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            channel.putLong( logCompactionInfo.leaderTerm() );
            channel.putLong( logCompactionInfo.prevIndex() );
            return null;
        }

        @Override
        public Void handle( RaftMessages.Timeout.Election election )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.PruneRequest pruneRequest )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest ) throws Exception
        {
            throw new UnsupportedOperationException( "Leadership Transfer extension is not supported with Raft protocol v2!" );
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal ) throws Exception
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection ) throws Exception
        {
            throw new UnsupportedOperationException( "Leadership Transfer extension is not supported with Raft protocol v2!" );
        }

        @Override
        public Void handle( RaftMessages.StatusResponse statusResponse ) throws Exception
        {
            throw new UnsupportedOperationException( "Status response is not supported with Raft protocol v2!" );
        }
    }
}
