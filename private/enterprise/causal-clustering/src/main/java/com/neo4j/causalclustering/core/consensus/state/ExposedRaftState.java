/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

public interface ExposedRaftState
{
    long lastLogIndexBeforeWeBecameLeader();

    long leaderCommit();

    long commitIndex();

    long appendIndex();

    long term();

    Set<MemberId> votingMembers();
}
