/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.dbms.identity.ServerId;

public class RaftTestMember
{
    private static final Map<Integer,RaftMemberId> raftMemberIds = new HashMap<>();
    private static final Map<Integer,ServerId> serverIds = new HashMap<>();

    private RaftTestMember()
    {
    }

    public static ServerId server( int id )
    {
        return serverIds.computeIfAbsent( id, k -> IdFactory.randomServerId() );
    }

    public static RaftMemberId raftMember( int id )
    {
        return raftMemberIds.computeIfAbsent( id, k -> IdFactory.randomRaftMemberId() );
    }

    public static LeaderInfo leader( int id, long term )
    {
        var member = raftMemberIds.computeIfAbsent( id, k -> IdFactory.randomRaftMemberId() );
        return new LeaderInfo( member, term );
    }
}
