/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source;

import com.codahale.metrics.Counter;

import org.neo4j.kernel.monitoring.ByteCounterMonitor;

public class ByteCountsMetric implements ByteCounterMonitor
{
    private final Counter bytesWritten = new Counter();
    private final Counter bytesRead = new Counter();

    public long getBytesWritten()
    {
        return bytesWritten.getCount();
    }

    public long getBytesRead()
    {
        return bytesRead.getCount();
    }

    @Override
    public void bytesWritten( long bytes )
    {
        bytesWritten.inc( bytes );
    }

    @Override
    public void bytesRead( long bytes )
    {
        bytesRead.inc( bytes );
    }
}
