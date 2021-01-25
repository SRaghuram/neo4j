/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.collection.CircularBuffer;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * Keeps elements over a limited consecutive range cached.
 *
 * @param <V> The type of element to cache.
 */
class ConsecutiveCache<V>
{
    private final CircularBuffer<V> circle;
    private long endIndex = -1;

    ConsecutiveCache( int capacity )
    {
        this.circle = new CircularBuffer<>( capacity );
    }

    private long firstIndex()
    {
        return endIndex - circle.size() + 1;
    }

    void put( long idx, V e, V[] evictions )
    {
        if ( idx < 0 )
        {
            throw new IllegalArgumentException( format( "Index must be >= 0 (was %d)", idx ) );
        }
        if ( e == null )
        {
            throw new IllegalArgumentException( "Null entries are not accepted" );
        }

        if ( idx == endIndex + 1 )
        {
            evictions[0] = circle.append( e );
            endIndex = endIndex + 1;
        }
        else
        {
            circle.clear( evictions );
            circle.append( e );
            endIndex = idx;
        }
    }

    V get( long idx )
    {
        if ( idx < 0 )
        {
            throw new IllegalArgumentException( format( "Index must be >= 0 (was %d)", idx ) );
        }

        if ( idx > endIndex || idx < firstIndex() )
        {
            return null;
        }

        return circle.read( toIntExact( idx - firstIndex() ) );
    }

    public void clear( V[] evictions )
    {
        circle.clear( evictions );
    }

    public int size()
    {
        return circle.size();
    }

    public void prune( long upToIndex, V[] evictions )
    {
        long index = firstIndex();
        int i = 0;
        while ( index <= min( upToIndex, endIndex ) )
        {
            evictions[i] = circle.remove();
            assert evictions[i] != null;
            i++;
            index++;
        }
    }

    public V remove()
    {
        return circle.remove();
    }

    public void truncate( long fromIndex, V[] evictions )
    {
        if ( fromIndex > endIndex )
        {
            return;
        }
        long index = max( fromIndex, firstIndex() );
        int i = 0;
        while ( index <= endIndex )
        {
            evictions[i++] = circle.removeHead();
            index++;
        }
        endIndex = fromIndex - 1;
    }
}
