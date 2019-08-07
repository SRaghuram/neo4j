/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.query;

import org.junit.jupiter.api.Test;

import org.neo4j.collection.Dependencies;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryEngineProviderTest
{
    @Test
    void shouldUsePickTheEngineWithLowestPriority()
    {
        // Given
        QueryEngineProvider provider1 = mock( QueryEngineProvider.class );
        QueryEngineProvider provider2 = mock( QueryEngineProvider.class );
        when( provider1.enginePriority() ).thenReturn( 1 );
        when( provider2.enginePriority() ).thenReturn( 2 );
        // When
        Iterable<QueryEngineProvider> providers = Iterables.asIterable( provider1, provider2 );
        QueryEngineProvider provider = QueryEngineProvider.chooseAlternative( providers );

        // Then
        assertSame( provider1, provider );
    }

    @Test
    void shouldPickTheOneAndOnlyQueryEngineAvailable()
    {
        // Given
        QueryEngineProvider provider1 = mock( QueryEngineProvider.class );
        when( provider1.enginePriority() ).thenReturn( 1 );

        // When
        Iterable<QueryEngineProvider> providers = Iterables.asIterable( provider1 );
        QueryEngineProvider provider = QueryEngineProvider.chooseAlternative( providers );

        // Then
        assertSame( provider1, provider );
    }

    @Test
    void shouldReturnNullOfNoQueryEngineAvailable()
    {
        // When
        QueryEngineProvider provider = QueryEngineProvider.chooseAlternative( Iterables.empty() );

        // Then
        assertNull( provider );
    }
}
