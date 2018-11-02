/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.explorer;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ComparableRaftStateTest
{
    @Test
    public void twoIdenticalStatesShouldBeEqual()
    {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        ComparableRaftState state1 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ), false, new InMemoryRaftLog(), new ConsecutiveInFlightCache(), logProvider );

        ComparableRaftState state2 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ), false, new InMemoryRaftLog(), new ConsecutiveInFlightCache(), logProvider );

        // then
        assertEquals(state1, state2);
    }
}
