/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;

public class MemberIdSetBuilder implements RaftMembers.Builder<RaftMemberId>
{
    @Override
    public RaftMembers<RaftMemberId> build( Set<RaftMemberId> members )
    {
        return new MemberIdSet( members );
    }
}
