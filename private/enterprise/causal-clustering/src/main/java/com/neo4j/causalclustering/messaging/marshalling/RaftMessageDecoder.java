/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.catchup.Protocol;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.marshal.EndOfStreamException;

public class RaftMessageDecoder extends ByteToMessageDecoder
{
    private final Protocol<ContentType> protocol;

    public RaftMessageDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    public void decode( ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list ) throws Exception
    {
        ReadableChannel channel = new NetworkReadableChannel( buffer );
        RaftGroupId raftGroupId = RaftGroupId.Marshal.INSTANCE.unmarshal( channel );

        int messageTypeWire = channel.getInt();
        RaftMessages.Type[] values = RaftMessages.Type.values();
        RaftMessages.Type messageType = values[messageTypeWire];

        RaftMemberId from = retrieveMember( channel );
        final LazyComposer composer;

        composer = getLazyComposer( channel, messageType, from )
                .orElseThrow(() -> new IllegalArgumentException( "Unknown message type" ));

        list.add( new InboundRaftMessageContainerComposer( composer, raftGroupId ) );
        protocol.expect( ContentType.ContentType );
    }

    protected Optional<LazyComposer> getLazyComposer( ReadableChannel channel, RaftMessages.Type messageType, RaftMemberId from )
            throws IOException, EndOfStreamException
    {
        switch ( messageType )
        {
        case VOTE_REQUEST:
        {
            return Optional.of( handleVoteRequest( channel, from ) );
        }
        case VOTE_RESPONSE:
        {
            return Optional.of( handleVoteResponse( channel, from ) );
        }
        case PRE_VOTE_REQUEST:
        {
            return Optional.of( handlePreVoteRequest( channel, from ) );
        }
        case PRE_VOTE_RESPONSE:
        {
            return Optional.of( handlePreVoteResponse( channel, from ) );
        }
        case APPEND_ENTRIES_REQUEST:
        {
            return Optional.of( handleAppendEntriesRequest( channel, from ) );
        }
        case APPEND_ENTRIES_RESPONSE:
        {
            return Optional.of( handleAppendEntriesResponse( channel, from ) );
        }
        case NEW_ENTRY_REQUEST:
        {
            return Optional.of( handleNewEntryRequest( channel, from ) );
        }
        case HEARTBEAT:
        {
            return Optional.of( handleHeartbeatRequest( channel, from ) );
        }
        case HEARTBEAT_RESPONSE:
        {
            return Optional.of( handleHeartbeatResponse( channel, from ) );
        }
        case LOG_COMPACTION_INFO:
        {
            return Optional.of(handleCompactionInfo( channel, from ));
        }
        default:
        {
            return Optional.empty();
        }
        }
    }

    private LazyComposer handleCompactionInfo( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long leaderTerm = channel.getLong();
        long prevIndex = channel.getLong();

        return new SimpleMessageComposer( new RaftMessages.LogCompactionInfo( from, leaderTerm, prevIndex ) );
    }

    private LazyComposer handleHeartbeatResponse( ReadableChannel channel, RaftMemberId from )
    {
        return new SimpleMessageComposer( new RaftMessages.HeartbeatResponse( from ) );
    }

    private LazyComposer handleHeartbeatRequest( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long leaderTerm = channel.getLong();
        long commitIndexTerm = channel.getLong();
        long commitIndex = channel.getLong();

        return new SimpleMessageComposer( new RaftMessages.Heartbeat( from, leaderTerm, commitIndex, commitIndexTerm ) );
    }

    private LazyComposer handleNewEntryRequest( ReadableChannel channel, RaftMemberId from )
    {
        return new NewEntryRequestComposer( from );
    }

    private LazyComposer handleAppendEntriesResponse( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long term = channel.getLong();
        boolean success = channel.get() == 1;
        long matchIndex = channel.getLong();
        long appendIndex = channel.getLong();

        return new SimpleMessageComposer( new RaftMessages.AppendEntries.Response( from, term, success, matchIndex, appendIndex ) );
    }

    private LazyComposer handleVoteRequest( ReadableChannel channel, RaftMemberId from ) throws IOException, EndOfStreamException
    {
        RaftMemberId candidate = retrieveMember( channel );

        long term = channel.getLong();
        long lastLogIndex = channel.getLong();
        long lastLogTerm = channel.getLong();

        return new SimpleMessageComposer( new RaftMessages.Vote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
    }

    private LazyComposer handleVoteResponse( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long term = channel.getLong();
        boolean voteGranted = channel.get() == 1;

        return new SimpleMessageComposer( new RaftMessages.Vote.Response( from, term, voteGranted ) );
    }

    private LazyComposer handlePreVoteRequest( ReadableChannel channel, RaftMemberId from ) throws IOException, EndOfStreamException
    {
        RaftMemberId candidate = retrieveMember( channel );

        long term = channel.getLong();
        long lastLogIndex = channel.getLong();
        long lastLogTerm = channel.getLong();

        return new SimpleMessageComposer( new RaftMessages.PreVote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
    }

    private LazyComposer handlePreVoteResponse( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long term = channel.getLong();
        boolean voteGranted = channel.get() == 1;

        return new SimpleMessageComposer( new RaftMessages.PreVote.Response( from, term, voteGranted ) );
    }

    private LazyComposer handleAppendEntriesRequest( ReadableChannel channel, RaftMemberId from ) throws IOException
    {
        long term = channel.getLong();
        long prevLogIndex = channel.getLong();
        long prevLogTerm = channel.getLong();
        long leaderCommit = channel.getLong();
        int entryCount = channel.getInt();

        return new AppendEntriesComposer( entryCount, from, term, prevLogIndex, prevLogTerm, leaderCommit );
    }

    public static class InboundRaftMessageContainerComposer
    {
        private final LazyComposer composer;
        private final RaftGroupId raftGroupId;

        public InboundRaftMessageContainerComposer( LazyComposer composer, RaftGroupId raftGroupId )
        {
            this.composer = composer;
            this.raftGroupId = raftGroupId;
        }

        public Optional<RaftMessages.InboundRaftMessageContainer> maybeCompose( Clock clock, Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            return composer.maybeComplete( terms, contents )
                           .map( m -> RaftMessages.InboundRaftMessageContainer.of( clock.instant(), raftGroupId, m ) );
        }
    }

    protected RaftMemberId retrieveMember( ReadableChannel buffer ) throws IOException, EndOfStreamException
    {
        RaftMemberId.Marshal memberIdMarshal = RaftMemberId.Marshal.INSTANCE;
        return memberIdMarshal.unmarshal( buffer );
    }

    protected interface LazyComposer
    {
        /**
         * Builds the complete raft message if provided collections contain enough data for building the complete message.
         */
        Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents );
    }

    /**
     * A plain message without any more internal content.
     */
    public static class SimpleMessageComposer implements LazyComposer
    {
        private final RaftMessages.RaftMessage message;

        public SimpleMessageComposer( RaftMessages.RaftMessage message )
        {
            this.message = message;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            return Optional.of( message );
        }
    }

    public static class AppendEntriesComposer implements LazyComposer
    {
        private final int entryCount;
        private final RaftMemberId from;
        private final long term;
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final long leaderCommit;

        public AppendEntriesComposer( int entryCount, RaftMemberId from, long term, long prevLogIndex, long prevLogTerm, long leaderCommit )
        {
            this.entryCount = entryCount;
            this.from = from;
            this.term = term;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.leaderCommit = leaderCommit;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            if ( terms.size() < entryCount || contents.size() < entryCount )
            {
                return Optional.empty();
            }

            RaftLogEntry[] entries = new RaftLogEntry[entryCount];
            for ( int i = 0; i < entryCount; i++ )
            {
                long term = terms.remove();
                ReplicatedContent content = contents.remove();
                entries[i] = new RaftLogEntry( term, content );
            }
            return Optional.of( new RaftMessages.AppendEntries.Request( from, term, prevLogIndex, prevLogTerm, entries, leaderCommit ) );
        }
    }

    protected static class NewEntryRequestComposer implements LazyComposer
    {
        private final RaftMemberId from;

        NewEntryRequestComposer( RaftMemberId from )
        {
            this.from = from;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            if ( contents.isEmpty() )
            {
                return Optional.empty();
            }
            else
            {
                return Optional.of( new RaftMessages.NewEntry.Request( from, contents.remove() ) );
            }
        }
    }
}
