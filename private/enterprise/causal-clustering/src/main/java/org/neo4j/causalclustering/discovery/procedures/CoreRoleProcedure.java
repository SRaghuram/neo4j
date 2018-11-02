/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery.procedures;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.discovery.RoleInfo;

public class CoreRoleProcedure extends RoleProcedure
{
    private final RaftMachine raft;

    public CoreRoleProcedure( RaftMachine raft )
    {
        super();
        this.raft = raft;
    }

    @Override
    RoleInfo role()
    {
        return raft.isLeader() ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
    }
}
