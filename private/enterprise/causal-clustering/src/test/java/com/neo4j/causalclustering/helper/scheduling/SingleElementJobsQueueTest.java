/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SingleElementJobsQueueTest
{
    private final SingleElementJobsQueue<Object> queue = new SingleElementJobsQueue<>();

    @Test
    void shouldStartEmpty()
    {
        assertNull( queue.poll() );
    }

    @Test
    void shouldAddElement()
    {
        var o = new Object();
        queue.offer( o );
        assertEquals( o, queue.poll() );
        assertNull( queue.poll() );
    }

    @Test
    void shouldKeepFirstElement()
    {
        var o1 = new Object();
        var o2 = new Object();
        queue.offer( o1 );
        queue.offer( o2 );
        assertEquals( o1, queue.poll() );
        assertNull( queue.poll() );
    }

    @Test
    void shouldHandleMultipleElements()
    {
        var o1 = new Object();
        var o2 = new Object();

        queue.offer( o1 );
        assertEquals( o1, queue.poll() );

        queue.offer( o2 );
        assertEquals( o2, queue.poll() );
    }

    @Test
    void shouldClearElement()
    {
        queue.offer( new Object() );
        queue.clear();
        assertNull( queue.poll() );
    }
}
