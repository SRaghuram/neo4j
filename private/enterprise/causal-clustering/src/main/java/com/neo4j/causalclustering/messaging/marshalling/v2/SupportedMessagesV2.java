/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.marshalling.SupportedMessages;

public class SupportedMessagesV2 extends SupportedMessages
{
    @Override
    public Boolean handle( RaftMessages.Vote.Request request )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.Vote.Response response )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.PreVote.Request request )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.PreVote.Response response )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.AppendEntries.Request request )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.AppendEntries.Response response )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.Heartbeat heartbeat )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.HeartbeatResponse heartbeatResponse )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.LogCompactionInfo logCompactionInfo )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.Timeout.Election election )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.Timeout.Heartbeat heartbeat )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.NewEntry.Request request )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.NewEntry.BatchRequest batchRequest )
    {
        return true;
    }

    @Override
    public Boolean handle( RaftMessages.PruneRequest pruneRequest )
    {
        return true;
    }
}
