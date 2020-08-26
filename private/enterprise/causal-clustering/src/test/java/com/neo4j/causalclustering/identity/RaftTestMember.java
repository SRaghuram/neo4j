/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;

import java.util.HashMap;
import java.util.Map;

public class RaftTestMember
{
    private static final Map<Integer,RaftMemberId> testMembers = new HashMap<>();

    private RaftTestMember()
    {
    }

    public static MemberId member( int id )
    {
        return MemberId.of( raftMember( id ) );
    }

    public static RaftMemberId raftMember( int id )
    {
        return testMembers.computeIfAbsent( id, k -> IdFactory.randomRaftMemberId() );
    }

    public static LeaderInfo leader( int id, long term )
    {
        var member = testMembers.computeIfAbsent( id, k -> IdFactory.randomRaftMemberId() );
        return new LeaderInfo( member, term );
    }
}
