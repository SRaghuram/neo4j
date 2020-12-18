/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.status.Status;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.ServerGroupName;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static com.neo4j.causalclustering.core.consensus.RaftMessages.Type.HEARTBEAT_RESPONSE;
import static com.neo4j.causalclustering.core.consensus.RaftMessages.Type.PRUNE_REQUEST;
import static com.neo4j.causalclustering.core.consensus.RaftMessages.Type.STATUS_RESPONSE;
import static java.lang.String.format;

public interface RaftMessages
{
    interface Handler<T, E extends Exception>
    {
        T handle( Vote.Request request ) throws E;

        T handle( Vote.Response response ) throws E;

        T handle( PreVote.Request request ) throws E;

        T handle( PreVote.Response response ) throws E;

        T handle( AppendEntries.Request request ) throws E;

        T handle( AppendEntries.Response response ) throws E;

        T handle( Heartbeat heartbeat ) throws E;

        T handle( HeartbeatResponse heartbeatResponse ) throws E;

        T handle( LogCompactionInfo logCompactionInfo ) throws E;

        T handle( Timeout.Election election ) throws E;

        T handle( Timeout.Heartbeat heartbeat ) throws E;

        T handle( NewEntry.Request request ) throws E;

        T handle( NewEntry.BatchRequest batchRequest ) throws E;

        T handle( PruneRequest pruneRequest ) throws E;

        T handle( LeadershipTransfer.Proposal leadershipTransferProposal ) throws E;

        T handle( LeadershipTransfer.Request leadershipTransferRequest ) throws E;

        T handle( LeadershipTransfer.Rejection leadershipTransferRejection ) throws E;

        T handle( StatusResponse statusResponse ) throws E;
    }

    abstract class HandlerAdaptor<T, E extends Exception> implements Handler<T,E>
    {
        @Override
        public T handle( RaftMessages.Vote.Request request ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.Vote.Response response ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.PreVote.Request request ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.PreVote.Response response ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.AppendEntries.Request request ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.AppendEntries.Response response ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.Heartbeat heartbeat ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.HeartbeatResponse heartbeatResponse ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.LogCompactionInfo logCompactionInfo ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.Timeout.Election election ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws E
        {
            return null;
        }

        @Override
        public T handle( NewEntry.Request request ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.NewEntry.BatchRequest batchRequest ) throws E
        {
            return null;
        }

        @Override
        public T handle( RaftMessages.PruneRequest pruneRequest ) throws E
        {
            return null;
        }

        @Override
        public T handle( LeadershipTransfer.Proposal leadershipTransferProposal ) throws E
        {
            return null;
        }

        @Override
        public T handle( LeadershipTransfer.Request leadershipTransferRequest ) throws E
        {
            return null;
        }

        @Override
        public T handle( LeadershipTransfer.Rejection leadershipTransferRejection ) throws E
        {
            return null;
        }

        @Override
        public T handle( StatusResponse statusResponse ) throws E
        {
            return null;
        }
    }

    // Position is used to identify messages. Changing order will break upgrade paths.
    enum Type
    {
        VOTE_REQUEST,
        VOTE_RESPONSE,

        APPEND_ENTRIES_REQUEST,
        APPEND_ENTRIES_RESPONSE,

        HEARTBEAT,
        HEARTBEAT_RESPONSE,
        LOG_COMPACTION_INFO,

        // Timeouts
        ELECTION_TIMEOUT,
        HEARTBEAT_TIMEOUT,

        // TODO: Refactor, these are client-facing messages / api. Perhaps not public and instantiated through an api
        // TODO: method instead?
        NEW_ENTRY_REQUEST,
        NEW_BATCH_REQUEST,

        PRUNE_REQUEST,

        PRE_VOTE_REQUEST,
        PRE_VOTE_RESPONSE,

        LEADERSHIP_TRANSFER_REQUEST,
        LEADERSHIP_TRANSFER_PROPOSAL,
        LEADERSHIP_TRANSFER_REJECTION,

        STATUS_RESPONSE
    }

    class Directed
    {
        RaftMemberId to;
        RaftMessage message;

        public Directed( RaftMemberId to, RaftMessage message )
        {
            this.to = to;
            this.message = message;
        }

        public RaftMemberId to()
        {
            return to;
        }

        public RaftMessage message()
        {
            return message;
        }

        @Override
        public String toString()
        {
            return format( "Directed{to=%s, message=%s}", to, message );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Directed directed = (Directed) o;
            return Objects.equals( to, directed.to ) && Objects.equals( message, directed.message );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( to, message );
        }
    }

    interface Vote
    {
        class Request extends RaftMessage
        {
            private long term;
            private RaftMemberId candidate;
            private long lastLogIndex;
            private long lastLogTerm;

            public Request( RaftMemberId from, long term, RaftMemberId candidate, long lastLogIndex, long lastLogTerm )
            {
                super( from, Type.VOTE_REQUEST );
                this.term = term;
                this.candidate = candidate;
                this.lastLogIndex = lastLogIndex;
                this.lastLogTerm = lastLogTerm;
            }

            public long term()
            {
                return term;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Request request = (Request) o;
                return lastLogIndex == request.lastLogIndex &&
                       lastLogTerm == request.lastLogTerm &&
                       term == request.term &&
                       candidate.equals( request.candidate );
            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + candidate.hashCode();
                result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
                result = 31 * result + (int) (lastLogTerm ^ (lastLogTerm >>> 32));
                return result;
            }

            @Override
            public String toString()
            {
                return format( "Vote.Request from %s {term=%d, candidate=%s, lastAppended=%d, lastLogTerm=%d}",
                               from, term, candidate, lastLogIndex, lastLogTerm );
            }

            public long lastLogTerm()
            {
                return lastLogTerm;
            }

            public long lastLogIndex()
            {
                return lastLogIndex;
            }

            public RaftMemberId candidate()
            {
                return candidate;
            }
        }

        class Response extends RaftMessage
        {
            private long term;
            private boolean voteGranted;

            public Response( RaftMemberId from, long term, boolean voteGranted )
            {
                super( from, Type.VOTE_RESPONSE );
                this.term = term;
                this.voteGranted = voteGranted;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Response response = (Response) o;

                return term == response.term && voteGranted == response.voteGranted;
            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + (voteGranted ? 1 : 0);
                return result;
            }

            @Override
            public String toString()
            {
                return format( "Vote.Response from %s {term=%d, voteGranted=%s}", from, term, voteGranted );
            }

            public long term()
            {
                return term;
            }

            public boolean voteGranted()
            {
                return voteGranted;
            }
        }
    }

    interface PreVote
    {
        class Request extends RaftMessage
        {
            private long term;
            private RaftMemberId candidate;
            private long lastLogIndex;
            private long lastLogTerm;

            public Request( RaftMemberId from, long term, RaftMemberId candidate, long lastLogIndex, long lastLogTerm )
            {
                super( from, Type.PRE_VOTE_REQUEST );
                this.term = term;
                this.candidate = candidate;
                this.lastLogIndex = lastLogIndex;
                this.lastLogTerm = lastLogTerm;
            }

            public long term()
            {
                return term;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Request request = (Request) o;
                return lastLogIndex == request.lastLogIndex &&
                       lastLogTerm == request.lastLogTerm &&
                       term == request.term &&
                       candidate.equals( request.candidate );
            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + candidate.hashCode();
                result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
                result = 31 * result + (int) (lastLogTerm ^ (lastLogTerm >>> 32));
                return result;
            }

            @Override
            public String toString()
            {
                return format( "PreVote.Request from %s {term=%d, candidate=%s, lastAppended=%d, lastLogTerm=%d}",
                               from, term, candidate, lastLogIndex, lastLogTerm );
            }

            public long lastLogTerm()
            {
                return lastLogTerm;
            }

            public long lastLogIndex()
            {
                return lastLogIndex;
            }

            public RaftMemberId candidate()
            {
                return candidate;
            }
        }

        class Response extends RaftMessage
        {
            private long term;
            private boolean voteGranted;

            public Response( RaftMemberId from, long term, boolean voteGranted )
            {
                super( from, Type.PRE_VOTE_RESPONSE );
                this.term = term;
                this.voteGranted = voteGranted;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Response response = (Response) o;

                return term == response.term && voteGranted == response.voteGranted;
            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + (voteGranted ? 1 : 0);
                return result;
            }

            @Override
            public String toString()
            {
                return format( "PreVote.Response from %s {term=%d, voteGranted=%s}", from, term, voteGranted );
            }

            public long term()
            {
                return term;
            }

            public boolean voteGranted()
            {
                return voteGranted;
            }
        }
    }

    interface AppendEntries
    {
        class Request extends RaftMessage
        {
            private long leaderTerm;
            private long prevLogIndex;
            private long prevLogTerm;
            private RaftLogEntry[] entries;
            private long leaderCommit;

            public Request( RaftMemberId from, long leaderTerm, long prevLogIndex, long prevLogTerm, RaftLogEntry[] entries, long leaderCommit )
            {
                super( from, Type.APPEND_ENTRIES_REQUEST, true, true );
                Objects.requireNonNull( entries );
                assert !((prevLogIndex == -1 && prevLogTerm != -1) || (prevLogTerm == -1 && prevLogIndex != -1)) :
                        format( "prevLogIndex was %d and prevLogTerm was %d", prevLogIndex, prevLogTerm );
                this.entries = entries;
                this.leaderTerm = leaderTerm;
                this.prevLogIndex = prevLogIndex;
                this.prevLogTerm = prevLogTerm;
                this.leaderCommit = leaderCommit;
            }

            public long leaderTerm()
            {
                return leaderTerm;
            }

            public long prevLogIndex()
            {
                return prevLogIndex;
            }

            public long prevLogTerm()
            {
                return prevLogTerm;
            }

            public RaftLogEntry[] entries()
            {
                return entries;
            }

            public long leaderCommit()
            {
                return leaderCommit;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Request request = (Request) o;
                return Objects.equals( leaderTerm, request.leaderTerm ) &&
                       Objects.equals( prevLogIndex, request.prevLogIndex ) &&
                       Objects.equals( prevLogTerm, request.prevLogTerm ) &&
                       Objects.equals( leaderCommit, request.leaderCommit ) &&
                       Arrays.equals( entries, request.entries );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( leaderTerm, prevLogIndex, prevLogTerm, Arrays.hashCode( entries ), leaderCommit );
            }

            @Override
            public String toString()
            {
                return format( "AppendEntries.Request from %s {leaderTerm=%d, prevLogIndex=%d, " +
                               "prevLogTerm=%d, entry=%s, leaderCommit=%d}",
                               from, leaderTerm, prevLogIndex, prevLogTerm, Arrays.toString( entries ), leaderCommit );
            }
        }

        class Response extends RaftMessage
        {
            private long term;
            private boolean success;
            private long matchIndex;
            private long appendIndex;

            public Response( RaftMemberId from, long term, boolean success, long matchIndex, long appendIndex )
            {
                super( from, Type.APPEND_ENTRIES_RESPONSE );
                this.term = term;
                this.success = success;
                this.matchIndex = matchIndex;
                this.appendIndex = appendIndex;
            }

            public long term()
            {
                return term;
            }

            public boolean success()
            {
                return success;
            }

            public long matchIndex()
            {
                return matchIndex;
            }

            public long appendIndex()
            {
                return appendIndex;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                if ( !super.equals( o ) )
                {
                    return false;
                }
                Response response = (Response) o;
                return term == response.term &&
                       success == response.success &&
                       matchIndex == response.matchIndex &&
                       appendIndex == response.appendIndex;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), term, success, matchIndex, appendIndex );
            }

            @Override
            public String toString()
            {
                return format( "AppendEntries.Response from %s {term=%d, success=%s, matchIndex=%d, appendIndex=%d}",
                               from, term, success, matchIndex, appendIndex );
            }
        }
    }

    class Heartbeat extends RaftMessage
    {
        private long leaderTerm;
        private long commitIndex;
        private long commitIndexTerm;

        public Heartbeat( RaftMemberId from, long leaderTerm, long commitIndex, long commitIndexTerm )
        {
            super( from, Type.HEARTBEAT );
            this.leaderTerm = leaderTerm;
            this.commitIndex = commitIndex;
            this.commitIndexTerm = commitIndexTerm;
        }

        public long leaderTerm()
        {
            return leaderTerm;
        }

        public long commitIndex()
        {
            return commitIndex;
        }

        public long commitIndexTerm()
        {
            return commitIndexTerm;
        }

        @Override
        public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }

            Heartbeat heartbeat = (Heartbeat) o;

            return leaderTerm == heartbeat.leaderTerm &&
                   commitIndex == heartbeat.commitIndex &&
                   commitIndexTerm == heartbeat.commitIndexTerm;
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + (int) (leaderTerm ^ (leaderTerm >>> 32));
            result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
            result = 31 * result + (int) (commitIndexTerm ^ (commitIndexTerm >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Heartbeat from %s {leaderTerm=%d, commitIndex=%d, commitIndexTerm=%d}", from, leaderTerm,
                           commitIndex, commitIndexTerm );
        }
    }

    class HeartbeatResponse extends RaftMessage
    {

        public HeartbeatResponse( RaftMemberId from )
        {
            super( from, HEARTBEAT_RESPONSE );
        }

        @Override
        public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public String toString()
        {
            return "HeartbeatResponse{from=" + from + "}";
        }
    }

    class LogCompactionInfo extends RaftMessage
    {
        private long leaderTerm;
        private long prevIndex;

        public LogCompactionInfo( RaftMemberId from, long leaderTerm, long prevIndex )
        {
            super( from, Type.LOG_COMPACTION_INFO );
            this.leaderTerm = leaderTerm;
            this.prevIndex = prevIndex;
        }

        public long leaderTerm()
        {
            return leaderTerm;
        }

        public long prevIndex()
        {
            return prevIndex;
        }

        @Override
        public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }

            LogCompactionInfo other = (LogCompactionInfo) o;

            return leaderTerm == other.leaderTerm &&
                   prevIndex == other.prevIndex;
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + (int) (leaderTerm ^ (leaderTerm >>> 32));
            result = 31 * result + (int) (prevIndex ^ (prevIndex >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Log compaction from %s {leaderTerm=%d, prevIndex=%d}", from, leaderTerm, prevIndex );
        }
    }

    interface Timeout
    {
        class Election extends RaftMessage
        {
            public Election( RaftMemberId from )
            {
                super( from, Type.ELECTION_TIMEOUT );
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return "Timeout.Election{}";
            }
        }

        class Heartbeat extends RaftMessage
        {
            public Heartbeat( RaftMemberId from )
            {
                super( from, Type.HEARTBEAT_TIMEOUT );
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return "Timeout.Heartbeat{}";
            }
        }
    }

    interface NewEntry
    {
        class Request extends RaftMessage
        {
            private ReplicatedContent content;

            public Request( RaftMemberId from, ReplicatedContent content )
            {
                super( from, Type.NEW_ENTRY_REQUEST, true, false );
                this.content = content;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return format( "NewEntry.Request from %s {content=%s}", from, content );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Request request = (Request) o;

                return !(content != null ? !content.equals( request.content ) : request.content != null);
            }

            @Override
            public int hashCode()
            {
                return content != null ? content.hashCode() : 0;
            }

            public ReplicatedContent content()
            {
                return content;
            }
        }

        class BatchRequest extends RaftMessage
        {
            private final List<ReplicatedContent> batch;

            public BatchRequest( List<ReplicatedContent> batch )
            {
                super( null, Type.NEW_BATCH_REQUEST, true, false );
                this.batch = batch;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                if ( !super.equals( o ) )
                {
                    return false;
                }
                BatchRequest batchRequest = (BatchRequest) o;
                return Objects.equals( batch, batchRequest.batch );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), batch );
            }

            @Override
            public String toString()
            {
                return "BatchRequest{" +
                       "batch=" + batch +
                       '}';
            }

            public List<ReplicatedContent> contents()
            {
                return Collections.unmodifiableList( batch );
            }
        }
    }

    final class OutboundRaftMessageContainer<RM extends RaftMessage>
    {
        private final RaftGroupId raftGroupId;
        private final RM message;

        public static <RM extends RaftMessage> OutboundRaftMessageContainer<RM> of( RaftGroupId raftGroupId, RM message )
        {
            return new OutboundRaftMessageContainer<>( raftGroupId, message );
        }

        private OutboundRaftMessageContainer( RaftGroupId raftGroupId, RM message )
        {
            Objects.requireNonNull( message );
            this.raftGroupId = raftGroupId;
            this.message = message;
        }

        public RaftGroupId raftGroupId()
        {
            return raftGroupId;
        }

        public RM message()
        {
            return message;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            OutboundRaftMessageContainer<?> that = (OutboundRaftMessageContainer<?>) o;
            return Objects.equals( raftGroupId, that.raftGroupId ) && Objects.equals( message(), that.message() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( raftGroupId, message() );
        }

        @Override
        public String toString()
        {
            return format( "{raftGroupId: %s, message: %s}", raftGroupId, message() );
        }
    }

    final class InboundRaftMessageContainer<RM extends RaftMessage>
    {
        private final Instant receivedAt;
        private final RaftGroupId raftGroupId;
        private final RM message;

        public static <RM extends RaftMessage> InboundRaftMessageContainer<RM> of( Instant receivedAt, RaftGroupId raftGroupId, RM message )
        {
            return new InboundRaftMessageContainer<>( receivedAt, raftGroupId, message );
        }

        private InboundRaftMessageContainer( Instant receivedAt, RaftGroupId raftGroupId, RM message )
        {
            Objects.requireNonNull( message );
            this.raftGroupId = raftGroupId;
            this.receivedAt = receivedAt;
            this.message = message;
        }

        public Instant receivedAt()
        {
            return receivedAt;
        }

        public RaftGroupId raftGroupId()
        {
            return raftGroupId;
        }

        public RM message()
        {
            return message;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            InboundRaftMessageContainer<?> that = (InboundRaftMessageContainer<?>) o;
            return Objects.equals( receivedAt, that.receivedAt ) && Objects.equals( raftGroupId, that.raftGroupId )
                    && Objects.equals( message(), that.message() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( receivedAt, raftGroupId, message() );
        }

        @Override
        public String toString()
        {
            return format( "{raftGroupId: %s, receivedAt: %s, message: %s}", raftGroupId, receivedAt, message() );
        }
    }

    class PruneRequest extends RaftMessage
    {
        private final long pruneIndex;

        public PruneRequest( long pruneIndex )
        {
            super( null, PRUNE_REQUEST );
            this.pruneIndex = pruneIndex;
        }

        public long pruneIndex()
        {
            return pruneIndex;
        }

        @Override
        public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }
            PruneRequest that = (PruneRequest) o;
            return pruneIndex == that.pruneIndex;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( super.hashCode(), pruneIndex );
        }

        @Override
        public String toString()
        {
            return "PruneRequest{" +
                   "pruneIndex=" + pruneIndex +
                   '}';
        }
    }

    class StatusResponse extends RaftMessage
    {

        private final Status status;
        private final UUID requestId;

        public StatusResponse( RaftMemberId from, Status status, UUID requestId )
        {
            super( from, STATUS_RESPONSE );
            this.status = status;
            this.requestId = requestId;
        }

        public UUID getRequestId()
        {
            return requestId;
        }

        public Status getStatus()
        {
            return status;
        }

        @Override
        public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }
            StatusResponse that = (StatusResponse) o;
            return Objects.equals( status, that.status ) &&
                   Objects.equals( requestId, that.requestId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( super.hashCode(), status, requestId );
        }

        @Override
        public String toString()
        {
            return "StatusResponse{" +
                   "status=" + status +
                   ", messageId=" + requestId +
                   ", from=" + from +
                   '}';
        }
    }

    interface LeadershipTransfer
    {
        class Request extends RaftMessage
        {
            private final long previousIndex;
            private final long term;
            private final Set<ServerGroupName> groups;

            public Request( RaftMemberId from, long previousIndex, long term, Set<ServerGroupName> groups )
            {
                super( from, Type.LEADERSHIP_TRANSFER_REQUEST );
                this.previousIndex = previousIndex;
                this.term = term;
                this.groups = Set.copyOf( groups );
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object object )
            {
                if ( this == object )
                {
                    return true;
                }
                if ( object == null || getClass() != object.getClass() )
                {
                    return false;
                }
                if ( !super.equals( object ) )
                {
                    return false;
                }
                Request request = (Request) object;
                return previousIndex == request.previousIndex &&
                       term == request.term &&
                       Objects.equals( groups, request.groups );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), previousIndex, term, groups );
            }

            public long term()
            {
                return term;
            }

            public long previousIndex()
            {
                return previousIndex;
            }

            public Set<ServerGroupName> groups()
            {
                return groups;
            }

            @Override
            public String toString()
            {
                return "LeadershipTransferRequest{" +
                       "previousIndex=" + previousIndex +
                       ", term=" + term +
                       ", groups=" + groups +
                       '}';
            }
        }

        class Rejection extends RaftMessage
        {
            private final long previousIndex;
            private final long term;

            public Rejection( RaftMemberId from, long previousIndex, long term )
            {
                super( from, Type.LEADERSHIP_TRANSFER_REJECTION );
                this.previousIndex = previousIndex;
                this.term = term;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object object )
            {
                if ( this == object )
                {
                    return true;
                }
                if ( object == null || getClass() != object.getClass() )
                {
                    return false;
                }
                if ( !super.equals( object ) )
                {
                    return false;
                }
                Rejection rejection = (Rejection) object;
                return previousIndex == rejection.previousIndex &&
                       term == rejection.term;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), previousIndex, term );
            }

            @Override
            public String toString()
            {
                return "LeadershipTransferRejection{" +
                       "previousIndex=" + previousIndex +
                       ", term=" + term +
                       '}';
            }

            public long term()
            {
                return term;
            }

            public long previousIndex()
            {
                return previousIndex;
            }
        }

        class Proposal extends RaftMessage
        {
            private final RaftMemberId proposed;
            private final Set<ServerGroupName> priorityGroups;

            public Proposal( RaftMemberId from, RaftMemberId proposed, Set<ServerGroupName> priorityGroups )
            {
                super( from, Type.LEADERSHIP_TRANSFER_PROPOSAL );
                this.proposed = proposed;
                this.priorityGroups = Set.copyOf( priorityGroups );
            }

            public RaftMemberId proposed()
            {
                return proposed;
            }

            public Set<ServerGroupName> priorityGroups()
            {
                return priorityGroups;
            }

            @Override
            public <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return "Proposal{" +
                       "proposed=" + proposed +
                       ", priorityGroups=" + priorityGroups +
                       '}';
            }

            @Override
            public boolean equals( Object object )
            {
                if ( this == object )
                {
                    return true;
                }
                if ( object == null || getClass() != object.getClass() )
                {
                    return false;
                }
                if ( !super.equals( object ) )
                {
                    return false;
                }
                Proposal proposal = (Proposal) object;
                return Objects.equals( proposed, proposal.proposed ) &&
                       Objects.equals( priorityGroups, proposal.priorityGroups );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), proposed, priorityGroups );
            }
        }
    }

    abstract class RaftMessage
    {
        protected final RaftMemberId from;
        private final boolean containsData;
        private final boolean requiresOrdering;
        private final Type type;

        RaftMessage( RaftMemberId from, Type type, boolean containsData, boolean requiresOrdering )
        {
            this.from = from;
            this.type = type;
            this.containsData = containsData;
            this.requiresOrdering = requiresOrdering;
        }

        RaftMessage( RaftMemberId from, Type type )
        {
            this( from, type, false, false );
        }

        public RaftMemberId from()
        {
            return from;
        }

        public boolean containsData()
        {
            return containsData;
        }

        public boolean requiresOrdering()
        {
            return requiresOrdering;
        }

        public Type type()
        {
            return type;
        }

        public abstract <T, E extends Exception> T dispatch( Handler<T,E> handler ) throws E;

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RaftMessage that = (RaftMessage) o;
            return Objects.equals( from, that.from ) && type == that.type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( from, type );
        }
    }
}
