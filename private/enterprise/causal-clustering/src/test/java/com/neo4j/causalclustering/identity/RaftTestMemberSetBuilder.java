/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembers;
import com.neo4j.causalclustering.core.consensus.membership.RaftTestMembers;

import java.util.Set;

public class RaftTestMemberSetBuilder implements RaftMembers.Builder<RaftMemberId>
{
    public static final RaftTestMemberSetBuilder INSTANCE = new RaftTestMemberSetBuilder();

    private RaftTestMemberSetBuilder()
    {
    }

    @Override
    public RaftMembers<RaftMemberId> build( Set<RaftMemberId> members )
    {
        return new RaftTestMembers( members );
    }
}
