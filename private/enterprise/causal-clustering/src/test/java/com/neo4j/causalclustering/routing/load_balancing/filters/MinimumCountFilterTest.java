/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class MinimumCountFilterTest
{
    @Test
    void shouldFilterBelowCount()
    {
        // given
        MinimumCountFilter<Integer> minFilter = new MinimumCountFilter<>( 3 );

        Set<Integer> input = asSet( 1, 2 );

        // when
        Set<Integer> output = minFilter.apply( input );

        // then
        assertEquals( emptySet(), output );
    }

    @Test
    void shouldPassAtCount()
    {
        // given
        MinimumCountFilter<Integer> minFilter = new MinimumCountFilter<>( 3 );

        Set<Integer> input = asSet( 1, 2, 3 );

        // when
        Set<Integer> output = minFilter.apply( input );

        // then
        assertEquals( input, output );
    }

    @Test
    void shouldPassAboveCount()
    {
        // given
        MinimumCountFilter<Integer> minFilter = new MinimumCountFilter<>( 3 );

        Set<Integer> input = asSet( 1, 2, 3, 4 );

        // when
        Set<Integer> output = minFilter.apply( input );

        // then
        assertEquals( input, output );
    }
}
