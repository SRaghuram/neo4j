/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

public class MemberIdSetBuilder implements RaftGroup.Builder<MemberId>
{
    @Override
    public RaftGroup<MemberId> build( Set<MemberId> members )
    {
        return new MemberIdSet( members );
    }
}
