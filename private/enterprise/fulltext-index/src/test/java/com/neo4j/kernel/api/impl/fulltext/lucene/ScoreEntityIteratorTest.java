/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator.ScoreEntry;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScoreEntityIteratorTest
{
    @Test
    public void concatShouldReturnOrderedResults()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntry[]{entry( 3, 10 ), entry( 10, 3 ), entry( 12, 1 )} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{entry( 1, 12 ), entry( 5, 8 ), entry( 7, 6 ), entry( 8, 5 ), entry( 11, 2 )} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntry[]{entry( 2, 11 ), entry( 4, 9 ), entry( 6, 7 ), entry( 9, 4 )} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 12; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 13 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    private static ScoreEntry entry( long id, float s )
    {
        return new ScoreEntry( id, s );
    }

    @Test
    public void concatShouldHandleEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( emptyEntries() );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{entry( 1, 5 ), entry( 2, 4 ), entry( 3, 3 ), entry( 4, 2 ), entry( 5, 1 )} );
        ScoreEntityIterator three = iteratorOf( emptyEntries() );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 5; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 6 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    @Test
    public void concatShouldHandleAllEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( emptyEntries() );
        ScoreEntityIterator two = iteratorOf( emptyEntries() );
        ScoreEntityIterator three = iteratorOf( emptyEntries() );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        assertFalse( concat.hasNext() );
    }

    private static ScoreEntry[] emptyEntries()
    {
        return new ScoreEntry[]{};
    }

    private static ScoreEntityIterator iteratorOf( ScoreEntry[] input )
    {
        return new ScoreEntityIterator( null )
        {
            Iterator<ScoreEntry> entries = Arrays.asList( input ).iterator();

            @Override
            public boolean hasNext()
            {
                return entries.hasNext();
            }

            @Override
            public ScoreEntry next()
            {
                return entries.next();
            }
        };
    }
}
