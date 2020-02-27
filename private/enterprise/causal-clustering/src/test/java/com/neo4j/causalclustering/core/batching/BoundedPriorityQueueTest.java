/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.E_COUNT_EXCEEDED;
import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.E_SIZE_EXCEEDED;
import static com.neo4j.causalclustering.core.batching.BoundedPriorityQueue.Result.OK;
import static java.util.Comparator.comparingInt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedPriorityQueueTest
{
    private final Config BASE_CONFIG = new Config( 0, 5, 100 );
    private final Comparator<Integer> NO_PRIORITY = ( a, b ) -> 0;

    private final ThreadLocalRandom tlr = ThreadLocalRandom.current();

    @Test
    void shouldReportTotalCountAndSize()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        Assertions.assertEquals( 0, queue.bytes() );
        Assertions.assertEquals( 0, queue.count() );

        queue.offer( 10 );
        Assertions.assertEquals( 1, queue.count() );
        Assertions.assertEquals( 10, queue.bytes() );

        queue.offer( 20 );
        Assertions.assertEquals( 2, queue.count() );
        Assertions.assertEquals( 30, queue.bytes() );

        queue.poll();
        Assertions.assertEquals( 1, queue.count() );
        Assertions.assertEquals( 20, queue.bytes() );

        queue.poll();
        Assertions.assertEquals( 0, queue.count() );
        Assertions.assertEquals( 0, queue.bytes() );
    }

    @Test
    void shouldNotAllowMoreThanMaxBytes()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        Assertions.assertEquals( E_SIZE_EXCEEDED, queue.offer( 101 ) );
        Assertions.assertEquals( OK, queue.offer( 99 ) );
        Assertions.assertEquals( OK, queue.offer( 1 ) );
        Assertions.assertEquals( E_SIZE_EXCEEDED, queue.offer( 1 ) );
    }

    @Test
    void shouldAllowMinCountDespiteSizeLimit()
    {
        Config config = new Config( 2, 5, 100 );
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( config, Integer::longValue, NO_PRIORITY );

        Assertions.assertEquals( OK, queue.offer( 101 ) );
        Assertions.assertEquals( OK, queue.offer( 101 ) );
        Assertions.assertEquals( E_SIZE_EXCEEDED, queue.offer( 1 ) );
    }

    @Test
    void shouldAllowZeroSizedItemsDespiteSizeLimit()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        Assertions.assertEquals( OK, queue.offer( 100 ) );
        Assertions.assertEquals( E_SIZE_EXCEEDED, queue.offer( 1 ) );

        Assertions.assertEquals( OK, queue.offer( 0 ) );
        Assertions.assertEquals( OK, queue.offer( 0 ) );
    }

    @Test
    void shouldNotAllowMoreThanMaxCount()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        Assertions.assertEquals( OK, queue.offer( 1 ) );
        Assertions.assertEquals( OK, queue.offer( 1 ) );
        Assertions.assertEquals( OK, queue.offer( 1 ) );
        Assertions.assertEquals( OK, queue.offer( 1 ) );
        Assertions.assertEquals( OK, queue.offer( 1 ) );

        Assertions.assertEquals( E_COUNT_EXCEEDED, queue.offer( 1 ) );
    }

    @Test
    void shouldNotAllowMoreThanMaxCountDespiteZeroSize()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        Assertions.assertEquals( OK, queue.offer( 0 ) );
        Assertions.assertEquals( OK, queue.offer( 0 ) );
        Assertions.assertEquals( OK, queue.offer( 0 ) );
        Assertions.assertEquals( OK, queue.offer( 0 ) );
        Assertions.assertEquals( OK, queue.offer( 0 ) );

        Assertions.assertEquals( E_COUNT_EXCEEDED, queue.offer( 0 ) );
    }

    @Test
    void shouldPollWithPredicate()
    {
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( BASE_CONFIG, Integer::longValue, NO_PRIORITY );

        assertEquals( OK, queue.offer( 1 ) );
        assertEquals( OK, queue.offer( 2 ) );
        assertEquals( OK, queue.offer( 3 ) );
        assertEquals( 3, queue.count() );
        assertEquals( 6, queue.bytes() );

        assertEquals( Integer.valueOf( 1 ), queue.pollIf( v -> v.equals( 1 ) ).get() );
        assertEquals( 2, queue.count() );
        assertEquals( 5, queue.bytes() );

        assertTrue( queue.pollIf( v -> v.equals( 1 ) ).isEmpty() );
        assertEquals( 2, queue.count() );
        assertEquals( 5, queue.bytes() );

        assertEquals( Integer.valueOf( 2 ), queue.pollIf( v -> v.equals( 2 ) ).get() );
        assertEquals( 1, queue.count() );
        assertEquals( 3, queue.bytes() );

        assertEquals( Integer.valueOf( 3 ), queue.pollIf( v -> v.equals( 3 ) ).get() );
        assertEquals( 0, queue.count() );
        assertEquals( 0, queue.bytes() );
    }

    @Test
    void shouldRespectPriority()
    {
        int count = 100;
        Config config = new Config( 0, count, 0 );
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<>( config, i -> 0L, Integer::compare );

        List<Integer> list = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ )
        {
            list.add( i );
        }

        Collections.shuffle( list, tlr );
        list.forEach( queue::offer );

        for ( int i = 0; i < count; i++ )
        {
            Assertions.assertEquals( Optional.of( i ), queue.poll() );
        }
    }

    @Test
    void shouldHaveStablePriority()
    {
        int count = 100;
        int priorities = 3;

        Config config = new Config( 0, count, 0 );
        BoundedPriorityQueue<Element> queue = new BoundedPriorityQueue<>( config, i -> 0L,
                comparingInt( p -> p.priority ) );

        List<Element> insertionOrder = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ )
        {
            insertionOrder.add( new Element( tlr.nextInt( priorities ) ) );
        }

        Collections.shuffle( insertionOrder, tlr );
        insertionOrder.forEach( queue::offer );

        for ( int p = 0; p < priorities; p++ )
        {
            ArrayList<Element> filteredInsertionOrder = new ArrayList<>();
            for ( Element element : insertionOrder )
            {
                if ( element.priority == p )
                {
                    filteredInsertionOrder.add( element );
                }
            }

            for ( Element element : filteredInsertionOrder )
            {
                Assertions.assertEquals( Optional.of( element ), queue.poll() );
            }
        }
    }

    static class Element
    {
        int priority;

        Element( int priority )
        {
            this.priority = priority;
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( priority );
        }
    }
}
