/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v4.decoding;

import com.neo4j.causalclustering.catchup.Protocol;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.status.Status;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.UUIDMarshal;
import com.neo4j.causalclustering.messaging.marshalling.v2.ContentType;
import com.neo4j.configuration.ServerGroupName;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
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
        RaftId raftId = RaftId.Marshal.INSTANCE.unmarshal( channel );

        int messageTypeWire = channel.getInt();
        RaftMessages.Type[] values = RaftMessages.Type.values();
        RaftMessages.Type messageType = values[messageTypeWire];

        MemberId from = retrieveMember( channel );
        LazyComposer composer;

        if ( messageType.equals( RaftMessages.Type.VOTE_REQUEST ) )
        {
            MemberId candidate = retrieveMember( channel );

            long term = channel.getLong();
            long lastLogIndex = channel.getLong();
            long lastLogTerm = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.Vote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
        }
        else if ( messageType.equals( RaftMessages.Type.VOTE_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean voteGranted = channel.get() == 1;

            composer = new SimpleMessageComposer( new RaftMessages.Vote.Response( from, term, voteGranted ) );
        }
        else if ( messageType.equals( RaftMessages.Type.PRE_VOTE_REQUEST ) )
        {
            MemberId candidate = retrieveMember( channel );

            long term = channel.getLong();
            long lastLogIndex = channel.getLong();
            long lastLogTerm = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.PreVote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
        }
        else if ( messageType.equals( RaftMessages.Type.PRE_VOTE_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean voteGranted = channel.get() == 1;

            composer = new SimpleMessageComposer( new RaftMessages.PreVote.Response( from, term, voteGranted ) );
        }
        else if ( messageType.equals( RaftMessages.Type.APPEND_ENTRIES_REQUEST ) )
        {
            // how many
            long term = channel.getLong();
            long prevLogIndex = channel.getLong();
            long prevLogTerm = channel.getLong();
            long leaderCommit = channel.getLong();
            int entryCount = channel.getInt();

            composer = new AppendEntriesComposer( entryCount, from, term, prevLogIndex, prevLogTerm, leaderCommit );
        }
        else if ( messageType.equals( RaftMessages.Type.APPEND_ENTRIES_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean success = channel.get() == 1;
            long matchIndex = channel.getLong();
            long appendIndex = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.AppendEntries.Response( from, term, success, matchIndex, appendIndex ) );
        }
        else if ( messageType.equals( RaftMessages.Type.NEW_ENTRY_REQUEST ) )
        {
            composer = new NewEntryRequestComposer( from );
        }
        else if ( messageType.equals( RaftMessages.Type.HEARTBEAT ) )
        {
            long leaderTerm = channel.getLong();
            long commitIndexTerm = channel.getLong();
            long commitIndex = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.Heartbeat( from, leaderTerm, commitIndex, commitIndexTerm ) );
        }
        else if ( messageType.equals( RaftMessages.Type.HEARTBEAT_RESPONSE ) )
        {
            composer = new SimpleMessageComposer( new RaftMessages.HeartbeatResponse( from ) );
        }
        else if ( messageType.equals( RaftMessages.Type.LOG_COMPACTION_INFO ) )
        {
            long leaderTerm = channel.getLong();
            long prevIndex = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.LogCompactionInfo( from, leaderTerm, prevIndex ) );
        }
        else if ( messageType.equals( RaftMessages.Type.LEADERSHIP_TRANSFER_REQUEST ) )
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

            composer = new SimpleMessageComposer( new RaftMessages.LeadershipTransfer.Request( from, previousIndex, term, groups ) );
        }
        else if ( messageType.equals( RaftMessages.Type.LEADERSHIP_TRANSFER_REJECTION ) )
        {
            long previousIndex = channel.getLong();
            long term = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.LeadershipTransfer.Rejection( from, previousIndex, term ) );
        }
        else if ( messageType.equals( RaftMessages.Type.STATUS_RESPONSE ) )
        {
            var messageId = UUIDMarshal.INSTANCE.unmarshal( channel );
            String statusMessage = StringMarshal.unmarshal( channel );
            composer = new SimpleMessageComposer(
                    new RaftMessages.StatusResponse( from, new Status( Status.Message.valueOf( statusMessage ) ), messageId ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }

        list.add( new InboundRaftMessageContainerComposer( composer, raftId ) );
        protocol.expect( ContentType.ContentType );
    }

    static class InboundRaftMessageContainerComposer
    {
        private final LazyComposer composer;
        private final RaftId raftId;

        InboundRaftMessageContainerComposer( LazyComposer composer, RaftId raftId )
        {
            this.composer = composer;
            this.raftId = raftId;
        }

        Optional<RaftMessages.InboundRaftMessageContainer> maybeCompose( Clock clock, Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            return composer.maybeComplete( terms, contents )
                           .map( m -> RaftMessages.InboundRaftMessageContainer.of( clock.instant(), raftId, m ) );
        }
    }

    private MemberId retrieveMember( ReadableChannel buffer ) throws IOException, EndOfStreamException
    {
        MemberId.Marshal memberIdMarshal = new MemberId.Marshal();
        return memberIdMarshal.unmarshal( buffer );
    }

    interface LazyComposer
    {
        /**
         * Builds the complete raft message if provided collections contain enough data for building the complete message.
         */
        Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents );
    }

    /**
     * A plain message without any more internal content.
     */
    private static class SimpleMessageComposer implements LazyComposer
    {
        private final RaftMessages.RaftMessage message;

        private SimpleMessageComposer( RaftMessages.RaftMessage message )
        {
            this.message = message;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            return Optional.of( message );
        }
    }

    private static class AppendEntriesComposer implements LazyComposer
    {
        private final int entryCount;
        private final MemberId from;
        private final long term;
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final long leaderCommit;

        AppendEntriesComposer( int entryCount, MemberId from, long term, long prevLogIndex, long prevLogTerm, long leaderCommit )
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

    private static class NewEntryRequestComposer implements LazyComposer
    {
        private final MemberId from;

        NewEntryRequestComposer( MemberId from )
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
