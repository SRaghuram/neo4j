/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

public abstract class SupportedMessages implements RaftMessages.Handler<Boolean,Exception>
{
    public static final SupportedMessages SUPPORT_ALL = new SupportedMessages()
    {
    };

    private boolean defaultToTrue()
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.Vote.Request request )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.Vote.Response response )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.PreVote.Request request )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.PreVote.Response response )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.AppendEntries.Request request )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.AppendEntries.Response response )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.Heartbeat heartbeat )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.Timeout.Election election )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.NewEntry.Request request )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.PruneRequest pruneRequest )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
    {
        return defaultToTrue();
    }

    @Override
    public Boolean handle( RaftMessages.StatusResponse statusResponse ) throws Exception
    {
        return defaultToTrue();
    }
}
