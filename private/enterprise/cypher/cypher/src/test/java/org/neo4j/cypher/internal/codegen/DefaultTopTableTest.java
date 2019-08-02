/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultTopTableTest
{
    private static Long[] testValues = new Long[]{7L, 4L, 5L, 0L, 3L, 4L, 8L, 6L, 1L, 9L, 2L};

    private static long[] expectedValues = new long[]{0L, 1L, 2L, 3L, 4L, 4L, 5L, 6L, 7L, 8L, 9L};

    @Test
    void shouldHandleAddingMoreValuesThanCapacity()
    {
        DefaultTopTable table = new DefaultTopTable( 7 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        table.sort();

        Iterator<Object> iterator = table.iterator();

        for ( int i = 0; i < 7; i++ )
        {
            assertTrue( iterator.hasNext() );
            long value = (long) iterator.next();
            assertEquals( expectedValues[i], value );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldHandleWhenNotCompletelyFilledToCapacity()
    {
        DefaultTopTable table = new DefaultTopTable( 20 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        table.sort();

        Iterator<Object> iterator = table.iterator();

        for ( int i = 0; i < testValues.length; i++ )
        {
            assertTrue( iterator.hasNext() );
            long value = (long) iterator.next();
            assertEquals( expectedValues[i], value );
        }
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldHandleWhenEmpty()
    {
        DefaultTopTable table = new DefaultTopTable( 10 );

        table.sort();

        Iterator<Object> iterator = table.iterator();

        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldThrowOnInitializeToZeroCapacity()
    {
        assertThrows( IllegalArgumentException.class, () -> new DefaultTopTable( 0 ) );
    }

    @Test
    void shouldThrowOnInitializeToNegativeCapacity()
    {
        assertThrows( IllegalArgumentException.class, () -> new DefaultTopTable( -1 ) );
    }

    @Test
    void shouldThrowOnSortNotCalledBeforeIterator()
    {
        DefaultTopTable table = new DefaultTopTable( 5 );
        for ( Long i : testValues )
        {
            table.add( i );
        }

        // We forgot to call sort() here...
        assertThrows( IllegalStateException.class, table::iterator );
    }
}
