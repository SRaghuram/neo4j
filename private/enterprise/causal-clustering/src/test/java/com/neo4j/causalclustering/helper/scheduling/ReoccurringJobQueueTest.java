/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReoccurringJobQueueTest
{
    private final ReoccurringJobQueue<Object> queue = new ReoccurringJobQueue<>();

    @Test
    void shouldQueueUpSameJob()
    {
        // given
        var element = new Object();

        // when
        queue.offer( element );
        //then
        assertEquals( element, queue.poll() );
        assertNull( queue.poll() );

        // when
        queue.offer( element );
        queue.offer( element );
        queue.offer( element );
        //then
        assertEquals( element, queue.poll() );
        assertEquals( element, queue.poll() );
        assertEquals( element, queue.poll() );
        assertNull( queue.poll() );
    }

    @Test
    void shouldRespectLowerBound()
    {
        // given
        var element = new Object();

        // when
        queue.poll();
        queue.offer( element );

        // then
        assertEquals( element, queue.poll() );
        assertNull( queue.poll() );
    }

    @Test
    void shouldThrowAssertionErrorIfOtherElementIsOffered()
    {
        queue.offer( new Object() );
        assertThrows( AssertionError.class, () -> queue.offer( new Object() ) );
    }
}
