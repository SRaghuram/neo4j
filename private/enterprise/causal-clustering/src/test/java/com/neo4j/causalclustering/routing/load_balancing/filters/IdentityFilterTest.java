/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.Test;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class IdentityFilterTest
{
    @Test
    public void shouldNotFilter()
    {
        // given
        IdentityFilter<Object> identityFilter = IdentityFilter.as();

        // when
        Set<Object> input = unmodifiableSet( asSet( 1, 2, 3 ) );
        Set<Object> output = identityFilter.apply( input );

        // then
        assertEquals( input, output );
    }
}
