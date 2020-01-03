/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.BatchAppendLogEntries;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import com.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.logging.Log;

import static java.lang.String.format;

class Appending
{
    private Appending()
    {
    }

    static void handleAppendEntriesRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.AppendEntries.Request request, Log log ) throws IOException
    {
        if ( request.leaderTerm() < state.term() )
        {
            RaftMessages.AppendEntries.Response appendResponse = new RaftMessages.AppendEntries.Response(
                    state.myself(), state.term(), false, -1, state.entryLog().appendIndex() );

            outcome.addOutgoingMessage( new RaftMessages.Directed( request.from(), appendResponse ) );
            return;
        }

        outcome.setPreElection( false );
        outcome.setNextTerm( request.leaderTerm() );
        outcome.setLeader( request.from() );
        outcome.setLeaderCommit( request.leaderCommit() );

        if ( !Follower.logHistoryMatches( state, request.prevLogIndex(), request.prevLogTerm() ) )
        {
            assert request.prevLogIndex() > -1 && request.prevLogTerm() > -1;
            RaftMessages.AppendEntries.Response appendResponse = new RaftMessages.AppendEntries.Response(
                    state.myself(), request.leaderTerm(), false, -1, state.entryLog().appendIndex() );

            outcome.addOutgoingMessage( new RaftMessages.Directed( request.from(), appendResponse ) );
            return;
        }

        long baseIndex = request.prevLogIndex() + 1;
        int offset;

        /* Find possible truncation point. */
        for ( offset = 0; offset < request.entries().length; offset++ )
        {
            long logIndex = baseIndex + offset;
            long logTerm = state.entryLog().readEntryTerm( logIndex );

            if ( logIndex > state.entryLog().appendIndex() )
            {
                // entry doesn't exist because it's beyond the current log end, so we can go ahead and append
                break;
            }
            else if ( logIndex < state.entryLog().prevIndex() )
            {
                // entry doesn't exist because it's before the earliest known entry, so continue with the next one
                continue;
            }
            else if ( logTerm != request.entries()[offset].term() )
            {
                /*
                 * the entry's index falls within our current range and the term doesn't match what we know. We must
                 * truncate.
                 */
                if ( logIndex <= state.commitIndex() ) // first, assert that we haven't committed what we are about to truncate
                {
                    throw new IllegalStateException(
                            format( "Cannot truncate entry at index %d with term %d when commit index is at %d",
                                    logIndex, logTerm, state.commitIndex() ) );
                }
                outcome.addLogCommand( new TruncateLogCommand( logIndex ) );
                break;
            }
        }

        if ( offset < request.entries().length )
        {
            outcome.addLogCommand( new BatchAppendLogEntries( baseIndex, offset, request.entries() ) );
        }

        Follower.commitToLogOnUpdate(
                state, request.prevLogIndex() + request.entries().length, request.leaderCommit(), outcome );

        long endMatchIndex = request.prevLogIndex() + request.entries().length; // this is the index of the last incoming entry
        RaftMessages.AppendEntries.Response appendResponse = new RaftMessages.AppendEntries.Response(
                state.myself(), request.leaderTerm(), true, endMatchIndex, endMatchIndex );
        outcome.addOutgoingMessage( new RaftMessages.Directed( request.from(), appendResponse ) );
    }

    static void appendNewEntry( ReadableRaftState ctx, Outcome outcome, ReplicatedContent content ) throws IOException
    {
        long prevLogIndex = ctx.entryLog().appendIndex();
        long prevLogTerm = prevLogIndex == -1 ? -1 :
                prevLogIndex > ctx.lastLogIndexBeforeWeBecameLeader() ?
                        ctx.term() :
                        ctx.entryLog().readEntryTerm( prevLogIndex );

        RaftLogEntry newLogEntry = new RaftLogEntry( ctx.term(), content );

        outcome.addShipCommand( new ShipCommand.NewEntries( prevLogIndex, prevLogTerm, new RaftLogEntry[]{ newLogEntry } ) );
        outcome.addLogCommand( new AppendLogEntry( prevLogIndex + 1, newLogEntry ) );
    }

    static void appendNewEntries( ReadableRaftState ctx, Outcome outcome,
            Collection<ReplicatedContent> contents ) throws IOException
    {
        long prevLogIndex = ctx.entryLog().appendIndex();
        long prevLogTerm = prevLogIndex == -1 ? -1 :
                prevLogIndex > ctx.lastLogIndexBeforeWeBecameLeader() ?
                        ctx.term() :
                        ctx.entryLog().readEntryTerm( prevLogIndex );

        RaftLogEntry[] raftLogEntries = contents.stream().map( content -> new RaftLogEntry( ctx.term(), content ) )
                .toArray( RaftLogEntry[]::new );

        outcome.addShipCommand( new ShipCommand.NewEntries( prevLogIndex, prevLogTerm, raftLogEntries ) );
        outcome.addLogCommand( new BatchAppendLogEntries( prevLogIndex + 1, 0, raftLogEntries ) );
    }
}
