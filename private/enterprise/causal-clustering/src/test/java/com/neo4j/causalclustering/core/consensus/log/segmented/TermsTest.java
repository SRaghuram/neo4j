/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.function.LongFunction;

class TermsTest
{
    private Terms terms;

    @Test
    void shouldHaveCorrectInitialValues()
    {
        // given
        long prevIndex = 5;
        long prevTerm = 10;
        terms = new Terms( prevIndex, prevTerm );

        // then
        assertTermInRange( -1, prevIndex, index -> -1L );
        Assertions.assertEquals( prevTerm, terms.get( prevIndex ) );
        assertTermInRange( prevIndex + 1, prevIndex + 10, index -> -1L );
    }

    @Test
    void shouldReturnAppendedTerms()
    {
        // given
        terms = new Terms( -1, -1 );
        int count = 10;

        // when
        appendRange( 0, count, index -> index * 2L );

        // then
        assertTermInRange( 0, count, index -> index * 2L );
        Assertions.assertEquals( -1, terms.get( -1 ) );
        Assertions.assertEquals( -1, terms.get( count ) );
    }

    @Test
    void shouldReturnAppendedTermsLongerRanges()
    {
        terms = new Terms( -1, -1 );
        int count = 10;

        // when
        for ( long term = 0; term < count; term++ )
        {
            appendRange( term * count, (term + 1) * count, term );
        }

        // then
        for ( long term = 0; term < count; term++ )
        {
            assertTermInRange( term * count, (term + 1) * count, term );
        }
    }

    @Test
    void shouldOnlyAcceptInOrderIndexes()
    {
        // given
        long prevIndex = 3;
        long term = 3;
        terms = new Terms( prevIndex, term );

        try
        {
            // when
            terms.append( prevIndex, term );
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }

        terms.append( prevIndex + 1, term ); // should work fine
        terms.append( prevIndex + 2, term ); // should work fine
        terms.append( prevIndex + 3, term ); // should work fine

        try
        {
            // when
            terms.append( prevIndex + 5, term );
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }

        terms.append( prevIndex + 4, term ); // should work fine
        terms.append( prevIndex + 5, term ); // should work fine
        terms.append( prevIndex + 6, term ); // should work fine
    }

    @Test
    void shouldOnlyAcceptMonotonicTerms()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        terms.append( prevIndex + 1, term );
        terms.append( prevIndex + 2, term );
        terms.append( prevIndex + 3, term + 1 );
        terms.append( prevIndex + 4, term + 1 );
        terms.append( prevIndex + 5, term + 2 );
        terms.append( prevIndex + 6, term + 2 );

        // when
        try
        {
            terms.append( prevIndex + 7, term + 1 );
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }
    }

    @Test
    void shouldNotTruncateNegativeIndexes()
    {
        // given
        terms = new Terms( -1, -1 );
        terms.append( 0, 0 );

        // when
        try
        {
            terms.truncate( -1 );
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }
    }

    @Test
    void shouldNotTruncateLessThanLowestIndex()
    {
        // given
        terms = new Terms( 5, 1 );

        // when
        try
        {
            terms.truncate( 4 );
            Assertions.fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }
    }

    @Test
    void shouldTruncateInCurrentRange()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, 20, term );
        Assertions.assertEquals( term, terms.get( 19 ) );

        // when
        long truncateFromIndex = 15;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, truncateFromIndex, index -> term );
        assertTermInRange( truncateFromIndex, 30, index -> -1L );
    }

    @Test
    void shouldTruncateAtExactBoundary()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 10;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, prevIndex + 10, term );
        assertTermInRange( prevIndex + 10, truncateFromIndex, -1 );
    }

    @Test
    void shouldTruncateCompleteCurrentRange()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // to be half-truncated
        appendRange( prevIndex + 20, prevIndex + 30, term + 2 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 15;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, prevIndex + 10, term );
        assertTermInRange( prevIndex + 10, truncateFromIndex, term + 1 );
        assertTermInRange( truncateFromIndex, prevIndex + 30, -1 );
    }

    @Test
    void shouldTruncateSeveralCompleteRanges()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // to be half-truncated
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // to be truncated
        appendRange( prevIndex + 20, prevIndex + 30, term + 2 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 5;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, truncateFromIndex, term );
        assertTermInRange( truncateFromIndex, prevIndex + 30, -1 );
    }

    @Test
    void shouldAppendAfterTruncate()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // to be half-truncated
        appendRange( prevIndex + 10, prevIndex + 20, term + 10 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 5;
        terms.truncate( truncateFromIndex );
        appendRange( truncateFromIndex, truncateFromIndex + 20, term + 20 );

        // then
        assertTermInRange( prevIndex + 1, truncateFromIndex, term );
        assertTermInRange( truncateFromIndex, truncateFromIndex + 20, term + 20 );
    }

    @Test
    void shouldAppendAfterSkip()
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 );

        // when
        long skipIndex = 30;
        long skipTerm = term + 2;
        terms.skip( skipIndex, skipTerm );

        // then
        assertTermInRange( prevIndex, skipIndex, -1 );
        Assertions.assertEquals( skipTerm, terms.get( skipIndex ) );

        // when
        appendRange( skipIndex + 1, skipIndex + 20, skipTerm );

        // then
        assertTermInRange( skipIndex + 1, skipIndex + 20, skipTerm );
    }

    @Test
    void shouldNotPruneAnythingIfBeforeMin() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 2, getIndexesSize() );
        Assertions.assertEquals( 2, getTermsSize() );

        // when
        terms.prune( prevIndex );

        // then
        assertTermInRange( prevIndex - 10, prevIndex, -1 );
        assertTermInRange( prevIndex, prevIndex + 10, term );
        assertTermInRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 2, getIndexesSize() );
        Assertions.assertEquals( 2, getTermsSize() );
    }

    @Test
    void shouldPruneInMiddleOfFirstRange() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // half-pruned
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 2, getIndexesSize() );
        Assertions.assertEquals( 2, getTermsSize() );

        // when
        long pruneIndex = prevIndex + 5;
        terms.prune( pruneIndex );

        // then
        assertTermInRange( prevIndex - 10, pruneIndex, -1 );
        assertTermInRange( pruneIndex, prevIndex + 10, term );
        assertTermInRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 2, getIndexesSize() );
        Assertions.assertEquals( 2, getTermsSize() );
    }

    @Test
    void shouldPruneAtBoundaryOfRange() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // completely pruned
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 2, getIndexesSize() );
        Assertions.assertEquals( 2, getTermsSize() );

        // when
        long pruneIndex = prevIndex + 10;
        terms.prune( pruneIndex );

        // then
        assertTermInRange( prevIndex - 10, pruneIndex, -1 );
        assertTermInRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 1, getIndexesSize() );
        Assertions.assertEquals( 1, getTermsSize() );
    }

    @Test
    void shouldPruneJustBeyondBoundaryOfRange() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // completely pruned
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 2, getIndexesSize() );
        Assertions.assertEquals( 2, getTermsSize() );

        // when
        long pruneIndex = prevIndex + 11;
        terms.prune( pruneIndex );

        // then
        assertTermInRange( prevIndex - 10, pruneIndex, -1 );
        assertTermInRange( prevIndex + 11, prevIndex + 20, term + 1 );

        Assertions.assertEquals( 1, getIndexesSize() );
        Assertions.assertEquals( 1, getTermsSize() );
    }

    @Test
    void shouldPruneSeveralCompleteRanges() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // completely pruned
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // completely pruned
        appendRange( prevIndex + 20, prevIndex + 30, term + 2 ); // half-pruned
        appendRange( prevIndex + 30, prevIndex + 40, term + 3 );
        appendRange( prevIndex + 40, prevIndex + 50, term + 4 );

        Assertions.assertEquals( 5, getIndexesSize() );
        Assertions.assertEquals( 5, getTermsSize() );

        // when
        long pruneIndex = prevIndex + 25;
        terms.prune( pruneIndex );

        // then
        assertTermInRange( prevIndex - 10, pruneIndex, -1 );
        assertTermInRange( pruneIndex, prevIndex + 30, term + 2 );
        assertTermInRange( prevIndex + 30, prevIndex + 40, term + 3 );
        assertTermInRange( prevIndex + 40, prevIndex + 50, term + 4 );

        Assertions.assertEquals( 3, getIndexesSize() );
        Assertions.assertEquals( 3, getTermsSize() );
    }

    @Test
    void shouldAppendNewItemsIfThereAreNoEntries() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        // when
        terms.truncate( prevIndex );

        // then
        Assertions.assertEquals( -1, terms.get( prevIndex ) );
        Assertions.assertEquals( -1, terms.latest() );
        Assertions.assertEquals( 0, getIndexesSize() );
        Assertions.assertEquals( 0, getTermsSize() );

        // and when
        terms.append( prevIndex, 5 );

        // then
        Assertions.assertEquals( term, terms.get( prevIndex ) );
        Assertions.assertEquals( term, terms.latest() );
        Assertions.assertEquals( 1, getIndexesSize() );
        Assertions.assertEquals( 1, getTermsSize() );
    }

    private int getTermsSize() throws NoSuchFieldException, IllegalAccessException
    {
        return getField( "terms" );
    }

    private int getIndexesSize() throws NoSuchFieldException, IllegalAccessException
    {
        return getField( "indexes" );
    }

    private int getField( String name ) throws NoSuchFieldException, IllegalAccessException
    {
        Field field = Terms.class.getDeclaredField( name );
        field.setAccessible( true );
        long[] longs = (long[]) field.get( terms );
        return longs.length;
    }

    private void assertTermInRange( long from, long to, long expectedTerm )
    {
        assertTermInRange( from, to, index -> expectedTerm );
    }

    private void assertTermInRange( long from, long to, LongFunction<Long> expectedTermFunction )
    {
        for ( long index = from; index < to; index++ )
        {
            Assertions.assertEquals( (long) expectedTermFunction.apply( index ), terms.get( index ), "For index: " + index );
        }
    }

    private void appendRange( long from, long to, long term )
    {
        appendRange( from, to, index -> term );
    }

    private void appendRange( long from, long to, LongFunction<Long> termFunction )
    {
        for ( long index = from; index < to; index++ )
        {
            terms.append( index, termFunction.apply( index ) );
        }
    }
}
