/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;
import java.util.UUID;

public class OutcomeTestBuilder
{
    private static final MemberId DEFAULT_LEADER = new MemberId( UUID.randomUUID() );

    public static OutcomeBuilder builder()
    {
        return new OutcomeBuilder( Role.FOLLOWER, 0, DEFAULT_LEADER, -1, null, false, false, -1, Set.of(), Set.of(), Set.of(), -1, new FollowerStates<>(), -1 );
    }
}