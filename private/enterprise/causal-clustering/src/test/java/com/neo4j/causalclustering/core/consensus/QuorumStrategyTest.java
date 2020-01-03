/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import org.junit.Test;

import static com.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuorumStrategyTest
{
    @Test
    public void shouldDecideIfWeHaveAMajorityCorrectly()
    {
        // the assumption in these tests is that we always vote for ourselves
        assertTrue( isQuorum( 0, 1, 0 ) );

        assertFalse( isQuorum( 0, 2, 0 ) );
        assertTrue( isQuorum( 0, 2, 1 ) );

        assertFalse( isQuorum( 0, 3, 0 ) );
        assertTrue( isQuorum( 0, 3, 1 ) );
        assertTrue( isQuorum( 0, 3, 2 ) );

        assertFalse( isQuorum( 0, 4, 0 ) );
        assertFalse( isQuorum( 0, 4, 1 ) );
        assertTrue( isQuorum( 0, 4, 2 ) );
        assertTrue( isQuorum( 0, 4, 3 ) );

        assertFalse( isQuorum( 0, 5, 0 ) );
        assertFalse( isQuorum( 0, 5, 1 ) );
        assertTrue( isQuorum( 0, 5, 2 ) );
        assertTrue( isQuorum( 0, 5, 3 ) );
        assertTrue( isQuorum( 0, 5, 4 ) );
    }

    @Test
    public void shouldDecideIfWeHaveAMajorityCorrectlyUsingMinQuorum()
    {
        // Then
        assertFalse( isQuorum( 2, 1, 0 ) );

        assertFalse( isQuorum( 2, 2, 0 ) );
        assertTrue( isQuorum( 2, 2, 1 ) );

        assertFalse( isQuorum( 2, 3, 0 ) );
        assertTrue( isQuorum( 2, 3, 1 ) );
        assertTrue( isQuorum( 2, 3, 2 ) );

        assertFalse( isQuorum( 2, 4, 0 ) );
        assertFalse( isQuorum( 2, 4, 1 ) );
        assertTrue( isQuorum( 2, 4, 2 ) );
        assertTrue( isQuorum( 2, 4, 3 ) );

        assertFalse( isQuorum( 2, 5, 0 ) );
        assertFalse( isQuorum( 2, 5, 1 ) );
        assertTrue( isQuorum( 2, 5, 2 ) );
        assertTrue( isQuorum( 2, 5, 3 ) );
        assertTrue( isQuorum( 2, 5, 4 ) );
    }
}
