/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

public class OutcomeTestBuilder
{
    private static final RaftMemberId DEFAULT_LEADER = IdFactory.randomRaftMemberId();

    public static OutcomeBuilder builder()
    {
        return new OutcomeBuilder( Role.FOLLOWER, 0, DEFAULT_LEADER, -1, null, null, false, -1, Set.of(), Set.of(), Set.of(), -1, new FollowerStates<>(), -1 );
    }
}
