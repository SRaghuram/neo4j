/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class FirstValidRuleTest
{
    @Test
    void shouldUseResultOfFirstNonEmpty()
    {
        // given
        Filter<Integer> removeValuesOfFive = data -> data.stream().filter( value -> value != 5 ).collect( Collectors.toSet() );
        Filter<Integer> countMoreThanFour = data -> data.size() > 4 ? data : Collections.emptySet();
        Filter<Integer> countMoreThanThree = data -> data.size() > 3 ? data : Collections.emptySet();

        FilterChain<Integer> ruleA = new FilterChain<>( asList( removeValuesOfFive, countMoreThanFour ) ); // should not succeed
        FilterChain<Integer> ruleB = new FilterChain<>( asList( removeValuesOfFive, countMoreThanThree ) ); // should succeed
        FilterChain<Integer> ruleC = new FilterChain<>( singletonList( countMoreThanFour ) ); // never reached

        FirstValidRule<Integer> firstValidRule = new FirstValidRule<>( asList( ruleA, ruleB, ruleC ) );

        Set<Integer> data = asSet( 5, 1, 5, 2, 5, 3, 5, 4 );

        // when
        data = firstValidRule.apply( data );

        // then
        assertEquals( asSet( 1, 2, 3, 4 ), data );
    }
}
