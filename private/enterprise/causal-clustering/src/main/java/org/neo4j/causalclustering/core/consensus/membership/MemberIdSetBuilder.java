/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.membership;

import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;

public class MemberIdSetBuilder implements RaftGroup.Builder<MemberId>
{
    @Override
    public RaftGroup<MemberId> build( Set<MemberId> members )
    {
        return new MemberIdSet( members );
    }
}
