/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class ComparableRaftStateTest
{
    @Test
    void twoIdenticalStatesShouldBeEqual()
    {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        ComparableRaftState state1 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ), false, false, new InMemoryRaftLog(), new ConsecutiveInFlightCache(), logProvider );

        ComparableRaftState state2 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ), false, false, new InMemoryRaftLog(), new ConsecutiveInFlightCache(), logProvider );

        // then
        assertEquals(state1, state2);
    }
}
