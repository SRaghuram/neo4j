/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.concurrent.TimeUnit;

class TimestampedValue
{
    private final long value;
    private final Long timestamp;
    private final TimeUnit timeUnit;

    TimestampedValue( long value, long timestamp, TimeUnit timeUnit )
    {
        this.value = value;
        this.timestamp = timestamp;
        this.timeUnit = timeUnit;
    }

    public long getValue()
    {
        return value;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    @Override
    public String toString()
    {
        return String.format( "%s<value=%s, timestamp=%d, timeUnit=%s>", TimestampedValue.class.getSimpleName(), value, timestamp, timeUnit );
    }
}
