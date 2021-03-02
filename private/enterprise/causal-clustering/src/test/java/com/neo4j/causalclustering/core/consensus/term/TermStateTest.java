/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.term;

import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

class TermStateTest implements BaseMarshalTest<TermState>
{
    @Override
    public Collection<TermState> originals()
    {
        return Stream.generate( () -> ThreadLocalRandom.current().nextLong( Long.MAX_VALUE ) )
                     .map( term ->
                     {
                         var ts = new TermState();
                         ts.update( term );
                         return ts;
                     } )
                     .limit( 5 )
                     .collect( Collectors.toList() );
    }

    @Override
    public ChannelMarshal<TermState> marshal()
    {
        return TermState.Marshal.INSTANCE;
    }

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
