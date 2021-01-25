/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.metric;

import com.codahale.metrics.Meter;

import java.util.function.LongSupplier;

public class MetricsCounter extends Meter
{
    private final LongSupplier countSupplier;
    private volatile long memo;

    public MetricsCounter( LongSupplier countSupplier )
    {
        this.countSupplier = countSupplier;
    }

    @Override
    public long getCount()
    {
        try
        {
            return memo = countSupplier.getAsLong();
        }
        catch ( Exception e )
        {
            return memo;
        }
    }
}
