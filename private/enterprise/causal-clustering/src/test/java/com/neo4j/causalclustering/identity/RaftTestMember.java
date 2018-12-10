/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

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
}
