/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.identity;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.membership.RaftGroup;
import org.neo4j.causalclustering.core.consensus.membership.RaftTestGroup;

import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class RaftTestMemberSetBuilder implements RaftGroup.Builder
{
    public static RaftTestMemberSetBuilder INSTANCE = new RaftTestMemberSetBuilder();

    private RaftTestMemberSetBuilder()
    {
    }

    @Override
    public RaftGroup build( Set members )
    {
        return new RaftTestGroup( members );
    }

    public static RaftGroup memberSet( int... ids )
    {
        HashSet members = new HashSet<>();
        for ( int id : ids )
        {
            members.add( member( id ) );
        }
        return new RaftTestGroup( members );
    }
}
