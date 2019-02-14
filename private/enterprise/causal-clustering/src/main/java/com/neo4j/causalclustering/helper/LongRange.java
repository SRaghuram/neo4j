/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import static java.lang.String.format;

public class LongRange
{
    public static LongRange range( long from, long to )
    {
        return new LongRange( from, to );
    }

    public static void assertIsRange( long from, long to )
    {
        if ( from < 0 )
        {
            throw new IllegalArgumentException( "Range cannot start from negative value. Got: " + from );
        }
        if ( to < from )
        {
            throw new IllegalArgumentException( format( "Not a valid range. RequiredTxId[%d] must be higher or equal to startTxId[%d].", to, from ) );
        }
    }

    private final long from;

    private final long to;

    private LongRange( long from, long to )
    {
        assertIsRange( from, to );
        this.from = from;
        this.to = to;
    }

    public boolean isWithinRange( long val )
    {
        return val >= from && val <= to;
    }

    @Override
    public String toString()
    {
        return "LongRange{" + "from=" + from + ", to=" + to + '}';
    }

    public long from()
    {
        return from;
    }

    public long to()
    {
        return to;
    }
}
