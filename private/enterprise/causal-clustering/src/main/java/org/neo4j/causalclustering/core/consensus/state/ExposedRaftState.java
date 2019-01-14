/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.state;

import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;

public interface ExposedRaftState
{
    long lastLogIndexBeforeWeBecameLeader();

    long leaderCommit();

    long commitIndex();

    long appendIndex();

    long term();

    Set<MemberId> votingMembers();
}
