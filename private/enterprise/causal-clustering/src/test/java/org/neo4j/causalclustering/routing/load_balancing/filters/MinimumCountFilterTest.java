/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.Test;

import java.util.Set;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class MinimumCountFilterTest
{
    @Test
    public void shouldFilterBelowCount()
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
    public void shouldPassAtCount()
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
    public void shouldPassAboveCount()
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
