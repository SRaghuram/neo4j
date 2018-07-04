/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;

/**
 * Iterator over entity ids together with their respective score.
 */
public class ScoreEntityIterator implements Iterator<ScoreEntityIterator.ScoreEntry>
{
    private final ValuesIterator iterator;
    private Runnable closeAction;

    ScoreEntityIterator( ValuesIterator sortedValuesIterator, Runnable closeAction )
    {
        this.iterator = sortedValuesIterator;
        this.closeAction = closeAction;
    }

    public Stream<ScoreEntry> stream()
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( this, Spliterator.ORDERED ), false );
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = iterator.hasNext();
        if ( !hasNext && closeAction != null )
        {
            closeAction.run();
            closeAction = null;
        }
        return hasNext;
    }

    @Override
    public ScoreEntry next()
    {
        if ( hasNext() )
        {
            return new ScoreEntry( iterator.next(), iterator.currentScore() );
        }
        else
        {
            throw new NoSuchElementException( "The iterator is exhausted" );
        }
    }

    /**
     * Concatenates the given iterators
     *
     * @param iterators to concatenate
     * @return a {@link ScoreEntityIterator} that iterates over all of the elements in all of the given iterators
     */
    static ScoreEntityIterator concat( List<ScoreEntityIterator> iterators )
    {
        return new ConcatenatingScoreEntityIterator( iterators );
    }

    private static class ConcatenatingScoreEntityIterator extends ScoreEntityIterator
    {
        private final List<? extends ScoreEntityIterator> iterators;
        private final ScoreEntry[] buffer;
        private boolean fetched;
        private ScoreEntry nextHead;

        ConcatenatingScoreEntityIterator( List<? extends ScoreEntityIterator> iterators )
        {
            super( null, null );
            this.iterators = iterators;
            this.buffer = new ScoreEntry[iterators.size()];
        }

        @Override
        public boolean hasNext()
        {
            if ( !fetched )
            {
                fetch();
            }
            return nextHead != null;
        }

        private void fetch()
        {
            int candidateHead = -1;
            for ( int i = 0; i < iterators.size(); i++ )
            {
                ScoreEntry entry = buffer[i];
                //Fill buffer if needed.
                if ( entry == null && iterators.get( i ).hasNext() )
                {
                    entry = iterators.get( i ).next();
                    buffer[i] = entry;
                }

                //Check if entry might be candidate for next to return.
                if ( entry != null && (nextHead == null || entry.score > nextHead.score) )
                {
                    nextHead = entry;
                    candidateHead = i;
                }
            }
            if ( candidateHead != -1 )
            {
                buffer[candidateHead] = null;
            }
            fetched = true;
        }

        @Override
        public ScoreEntry next()
        {
            if ( hasNext() )
            {
                fetched = false;
                ScoreEntry best = nextHead;
                nextHead = null;
                return best;
            }
            else
            {
                throw new NoSuchElementException( "The iterator is exhausted" );
            }
        }
    }

    /**
     * A ScoreEntry consists of an entity id together with its score.
     */
    public static class ScoreEntry
    {
        private final long entityId;
        private final float score;

        public long entityId()
        {
            return entityId;
        }

        public float score()
        {
            return score;
        }

        ScoreEntry( long entityId, float score )
        {

            this.entityId = entityId;
            this.score = score;
        }
    }
}
