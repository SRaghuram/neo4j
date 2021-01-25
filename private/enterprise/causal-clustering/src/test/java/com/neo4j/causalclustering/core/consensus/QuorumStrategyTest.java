/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;

class QuorumStrategyTest
{
    @Test
    void shouldDecideIfWeHaveAMajorityCorrectly()
    {
        // the assumption in these tests is that we always vote for ourselves
        Assertions.assertTrue( isQuorum( 0, 1, 0 ) );

        Assertions.assertFalse( isQuorum( 0, 2, 0 ) );
        Assertions.assertTrue( isQuorum( 0, 2, 1 ) );

        Assertions.assertFalse( isQuorum( 0, 3, 0 ) );
        Assertions.assertTrue( isQuorum( 0, 3, 1 ) );
        Assertions.assertTrue( isQuorum( 0, 3, 2 ) );

        Assertions.assertFalse( isQuorum( 0, 4, 0 ) );
        Assertions.assertFalse( isQuorum( 0, 4, 1 ) );
        Assertions.assertTrue( isQuorum( 0, 4, 2 ) );
        Assertions.assertTrue( isQuorum( 0, 4, 3 ) );

        Assertions.assertFalse( isQuorum( 0, 5, 0 ) );
        Assertions.assertFalse( isQuorum( 0, 5, 1 ) );
        Assertions.assertTrue( isQuorum( 0, 5, 2 ) );
        Assertions.assertTrue( isQuorum( 0, 5, 3 ) );
        Assertions.assertTrue( isQuorum( 0, 5, 4 ) );
    }

    @Test
    void shouldDecideIfWeHaveAMajorityCorrectlyUsingMinQuorum()
    {
        // Then
        Assertions.assertFalse( isQuorum( 2, 1, 0 ) );

        Assertions.assertFalse( isQuorum( 2, 2, 0 ) );
        Assertions.assertTrue( isQuorum( 2, 2, 1 ) );

        Assertions.assertFalse( isQuorum( 2, 3, 0 ) );
        Assertions.assertTrue( isQuorum( 2, 3, 1 ) );
        Assertions.assertTrue( isQuorum( 2, 3, 2 ) );

        Assertions.assertFalse( isQuorum( 2, 4, 0 ) );
        Assertions.assertFalse( isQuorum( 2, 4, 1 ) );
        Assertions.assertTrue( isQuorum( 2, 4, 2 ) );
        Assertions.assertTrue( isQuorum( 2, 4, 3 ) );

        Assertions.assertFalse( isQuorum( 2, 5, 0 ) );
        Assertions.assertFalse( isQuorum( 2, 5, 1 ) );
        Assertions.assertTrue( isQuorum( 2, 5, 2 ) );
        Assertions.assertTrue( isQuorum( 2, 5, 3 ) );
        Assertions.assertTrue( isQuorum( 2, 5, 4 ) );
    }
}
