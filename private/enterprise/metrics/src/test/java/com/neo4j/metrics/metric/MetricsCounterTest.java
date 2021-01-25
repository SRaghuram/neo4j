/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.metric;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsCounterTest
{
    @Test
    void getCountFromSupplier()
    {
        MetricsCounter counter = new MetricsCounter( new AtomicInteger()::incrementAndGet );
        assertEquals( 1, counter.getCount() );
        assertEquals( 2, counter.getCount() );
        assertEquals( 3, counter.getCount() );
    }

    @Test
    void mustReturnPriorMemoizedCountsOnException()
    {
        AtomicInteger integer = new AtomicInteger();
        AtomicBoolean throwing = new AtomicBoolean();

        MetricsCounter counter = new MetricsCounter( () ->
        {
            if ( throwing.get() )
            {
                throw new RuntimeException( "Boo" );
            }
            return integer.incrementAndGet();
        } );

        throwing.set( true );
        assertEquals( 0, counter.getCount() );
        throwing.set( false );
        assertEquals( 1, counter.getCount() );
        assertEquals( 2, counter.getCount() );
        throwing.set( true );
        assertEquals( 2, counter.getCount() );
    }
}
