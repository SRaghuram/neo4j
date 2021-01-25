/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.util.HashMap;
import java.util.Map;

class ProfileRefCounts
{
    private static class Counter
    {
        private int count;

        void increment()
        {
            count++;
        }

        int decrementAndGet()
        {
            return --count;
        }
    }

    private final Map<Profile,Counter> bag;

    ProfileRefCounts()
    {
        bag = new HashMap<>();
    }

    synchronized void incrementRefCounts( Profile[] profiles )
    {
        for ( Profile profile : profiles )
        {
            bag.computeIfAbsent( profile, p -> new Counter() ).increment();
        }
    }

    synchronized void decrementRefCounts( Profile[] profiles )
    {
        for ( Profile profile : profiles )
        {
            bag.computeIfPresent( profile, ( p, i ) -> i.decrementAndGet() == 0 ? null : i );
        }
    }

    synchronized boolean contains( Profile profile )
    {
        return bag.containsKey( profile );
    }
}
