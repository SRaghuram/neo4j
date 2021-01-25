/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

public abstract class SupportedMessages implements RaftMessages.Handler<Boolean,Exception>
{
    private final boolean defaultValue;

    protected SupportedMessages()
    {
        this( false );
    }

    private SupportedMessages( boolean defaultValue )
    {
        this.defaultValue = defaultValue;
    }

    public static final SupportedMessages SUPPORT_ALL = new SupportedMessages( true )
    {
    };

    private boolean defaultValue()
    {
        return defaultValue;
    }

    @Override
    public Boolean handle( RaftMessages.Vote.Request request )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.Vote.Response response )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.PreVote.Request request )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.PreVote.Response response )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.AppendEntries.Request request )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.AppendEntries.Response response )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.Heartbeat heartbeat )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.Timeout.Election election )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.NewEntry.Request request )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.PruneRequest pruneRequest )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Proposal leadershipTransferProposal )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Request leadershipTransferRequest )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.LeadershipTransfer.Rejection leadershipTransferRejection )
    {
        return defaultValue();
    }

    @Override
    public Boolean handle( RaftMessages.StatusResponse statusResponse )
    {
        return defaultValue();
    }
}
