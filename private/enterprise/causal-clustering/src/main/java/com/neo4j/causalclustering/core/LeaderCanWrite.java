/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;

import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.factory.AccessCapability;

import static java.lang.String.format;

public class LeaderCanWrite implements AccessCapability
{
    public static final String NOT_LEADER_ERROR_MSG =
            "No write operations are allowed directly on this database. Writes must pass through the leader. " +
            "The role of this server is: %s";

    private final RaftMachine raftMachine;

    public LeaderCanWrite( RaftMachine raftMachine )
    {
        this.raftMachine = raftMachine;
    }

    @Override
    public void assertCanWrite()
    {
        Role currentRole = raftMachine.currentRole();
        if ( !currentRole.equals( Role.LEADER ) )
        {
            throw new WriteOperationsNotAllowedException( format( NOT_LEADER_ERROR_MSG, currentRole ),
                    Status.Cluster.NotALeader );
        }
    }
}
