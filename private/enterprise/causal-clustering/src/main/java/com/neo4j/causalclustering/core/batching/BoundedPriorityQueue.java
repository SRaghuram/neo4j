/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.E_COUNT_EXCEEDED;
import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.E_SIZE_EXCEEDED;
import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.OK;

/**
 * A bounded queue which is bounded both by the count of elements and by the total
 * size of all elements. The queue also has a minimum count which allows the queue
 * to always allow a minimum number of items, regardless of total size.
 *
 * @param <E> element type
 */
public class BoundedPriorityQueue<E>
{
    public static class Config
    {
        private final int minCount;
        private final int maxCount;
        private final long maxBytes;

        public Config( int maxCount, long maxBytes )
        {
            this( 1, maxCount, maxBytes );
        }

        public Config( int minCount, int maxCount, long maxBytes )
        {
            this.minCount = minCount;
            this.maxCount = maxCount;
            this.maxBytes = maxBytes;
        }
    }

    public enum Result
    {
        OK,
        E_COUNT_EXCEEDED,
        E_SIZE_EXCEEDED
    }

    private final Config config;
    private final Function<E,Long> sizeOf;

    private final PriorityQueue<StableElement> queue;
    private final AtomicLong seqGen = new AtomicLong();
    private final AtomicInteger count = new AtomicInteger();
    private final AtomicLong bytes = new AtomicLong();

    BoundedPriorityQueue( Config config, Function<E,Long> sizeOf, java.util.Comparator<E> comparator )
    {
        this.config = config;
        this.sizeOf = sizeOf;
        this.queue = new PriorityQueue<>( config.maxCount, new Comparator( comparator ) );
    }

    public int count()
    {
        return count.get();
    }

    public long bytes()
    {
        return bytes.get();
    }

    /**
     * Offers an element to the queue which gets accepted if neither the
     * element count nor the total byte limits are broken.
     *
     * @param element The element offered.
     * @return OK if successful, and a specific error code otherwise.
     */
    public synchronized Result offer( E element )
    {
        int updatedCount = count.incrementAndGet();
        if ( updatedCount > config.maxCount )
        {
            count.decrementAndGet();
            return E_COUNT_EXCEEDED;
        }

        long elementBytes = sizeOf.apply( element );
        long updatedBytes = bytes.addAndGet( elementBytes );

        if ( elementBytes != 0 && updatedCount > config.minCount )
        {
            if ( updatedBytes > config.maxBytes )
            {
                bytes.addAndGet( -elementBytes );
                count.decrementAndGet();
                return E_SIZE_EXCEEDED;
            }
        }

        if ( !queue.offer( new StableElement( element ) ) )
        {
            // this should not happen because we already have a reservation
            throw new IllegalStateException();
        }

        return OK;
    }

    /**
     * Helper for deducting the element and byte counts for a removed element.
     */
    private Optional<E> deduct( StableElement element )
    {
        if ( element == null )
        {
            return Optional.empty();
        }
        count.decrementAndGet();
        bytes.addAndGet( -sizeOf.apply( element.element ) );
        return Optional.of( element.element );
    }

    public synchronized Optional<E> poll()
    {
        return deduct( queue.poll() );
    }

    synchronized Optional<E> pollIf( Predicate<E> predicate )
    {
        var nextElement = queue.peek();
        if ( nextElement != null && predicate.test( nextElement.element ) )
        {
            return poll();
        }

        return Optional.empty();
    }

    class StableElement
    {
        private final long seqNo = seqGen.getAndIncrement();
        private final E element;

        StableElement( E element )
        {
            this.element = element;
        }

        public E get()
        {
            return element;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            //noinspection unchecked
            var that = (StableElement) o;
            return seqNo == that.seqNo;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( seqNo );
        }
    }

    class Comparator implements java.util.Comparator<BoundedPriorityQueue<E>.StableElement>
    {
        private final java.util.Comparator<E> comparator;

        Comparator( java.util.Comparator<E> comparator )
        {
            this.comparator = comparator;
        }

        @Override
        public int compare( BoundedPriorityQueue<E>.StableElement o1, BoundedPriorityQueue<E>.StableElement o2 )
        {
            int compare = comparator.compare( o1.element, o2.element );
            if ( compare != 0 )
            {
                return compare;
            }
            return Long.compare( o1.seqNo, o2.seqNo );
        }
    }
}
