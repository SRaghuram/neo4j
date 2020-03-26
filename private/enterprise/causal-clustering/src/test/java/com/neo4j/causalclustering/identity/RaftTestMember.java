/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RaftTestMember
{
    private static final Map<Integer,MemberId> testMembers = new HashMap<>();

    private RaftTestMember()
    {
    }

    public static MemberId member( int id )
    {
        return testMembers.computeIfAbsent( id, k -> new MemberId( UUID.randomUUID() ) );
    }

    public static LeaderInfo leader( int id, long term )
    {
        var member = testMembers.computeIfAbsent( id, k -> new MemberId( UUID.randomUUID() ) );
        return new LeaderInfo( member, term );
    }
}
