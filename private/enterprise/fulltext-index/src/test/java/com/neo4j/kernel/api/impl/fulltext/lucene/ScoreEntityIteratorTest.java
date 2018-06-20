/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

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
        ScoreEntityIterator one = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{e( 3, 10 ), e( 10, 3 ), e( 12, 1 )} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{e( 1, 12 ), e( 5, 8 ), e( 7, 6 ), e( 8, 5 ), e( 11, 2 )} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{e( 2, 11 ), e( 4, 9 ), e( 6, 7 ), e( 9, 4 )} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 12; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntityIterator.ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 13 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    private ScoreEntityIterator.ScoreEntry e( long id, float s )
    {
        return new ScoreEntityIterator.ScoreEntry( id, s );
    }

    @Test
    public void concatShouldHandleEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{e( 1, 5 ), e( 2, 4 ), e( 3, 3 ), e( 4, 2 ), e( 5, 1 )} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 5; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntityIterator.ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 6 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    @Test
    public void concatShouldHandleAllEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntityIterator.ScoreEntry[]{} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        assertFalse( concat.hasNext() );
    }

    private ScoreEntityIterator iteratorOf( ScoreEntityIterator.ScoreEntry[] input )
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
