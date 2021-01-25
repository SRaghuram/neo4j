/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

import java.util.List;

public final class ClusterStatusResponse
{
    private final List<RaftMessages.StatusResponse> responses;
    private final ReplicationResult replicationResult;

    public ClusterStatusResponse( List<RaftMessages.StatusResponse> responses, ReplicationResult replicationResult )
    {
        this.responses = responses;
        this.replicationResult = replicationResult;
    }

    public ReplicationResult getReplicationResult()
    {
        return replicationResult;
    }

    public List<RaftMessages.StatusResponse> getResponses()
    {
        return responses;
    }
}
