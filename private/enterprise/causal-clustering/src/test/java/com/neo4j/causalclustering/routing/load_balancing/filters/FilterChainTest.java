/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class FilterChainTest
{
    @Test
    void shouldFilterThroughAll()
    {
        // given
        Filter<Integer> removeValuesOfFive = data -> data.stream().filter( value -> value != 5 ).collect( Collectors.toSet() );
        Filter<Integer> mustHaveThreeValues = data -> data.size() == 3 ? data : emptySet();
        Filter<Integer> keepValuesBelowTen = data -> data.stream().filter( value -> value < 10 ).collect( Collectors.toSet() );

        FilterChain<Integer> filterChain = new FilterChain<>( asList( removeValuesOfFive, mustHaveThreeValues, keepValuesBelowTen ) );
        Set<Integer> data = asSet( 5, 5, 5, 3, 5, 10, 9 ); // carefully crafted to check order as well

        // when
        data = filterChain.apply( data );

        // then
        assertEquals( asSet( 3, 9 ), data );
    }
}
