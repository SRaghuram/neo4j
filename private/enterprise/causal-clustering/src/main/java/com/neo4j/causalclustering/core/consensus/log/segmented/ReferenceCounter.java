/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps a reference count using CAS.
 *
 */
class ReferenceCounter
{
    private static final int DISPOSED_VALUE = -1;

    private AtomicInteger count = new AtomicInteger();

    boolean increase()
    {
        while ( true )
        {
            int pre = count.get();
            if ( pre == DISPOSED_VALUE )
            {
                return false;
            }
            else if ( count.compareAndSet( pre, pre + 1 ) )
            {
                return true;
            }
        }
    }

    void decrease()
    {
        while ( true )
        {
            int pre = count.get();
            if ( pre <= 0 )
            {
                throw new IllegalStateException( "Illegal count: " + pre );
            }
            else if ( count.compareAndSet( pre, pre - 1 ) )
            {
                return;
            }
        }
    }

    /**
     * Idempotently try to dispose this reference counter.
     *
     * @return True if the reference counter was or is now disposed.
     */
    boolean tryDispose()
    {
        return count.get() == DISPOSED_VALUE || count.compareAndSet( 0, DISPOSED_VALUE );
    }

    public int get()
    {
        return count.get();
    }
}
