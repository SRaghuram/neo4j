/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.term;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TermStateTest
{
    @Test
    void shouldStoreCurrentTerm()
    {
        // given
        TermState termState = new TermState();

        // when
        termState.update( 21 );

        // then
        Assertions.assertEquals( 21, termState.currentTerm() );
    }

    @Test
    void rejectLowerTerm()
    {
        // given
        TermState termState = new TermState();
        termState.update( 21 );

        // when
        try
        {
            termState.update( 20 );
            Assertions.fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }
}
