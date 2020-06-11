/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.util;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Duration
{
    private final long duration;
    private final TimeUnit unit;
    private final double variance;

    public Duration( long duration, TimeUnit unit )
    {
        this( duration, unit, 0.2D );
    }

    public Duration( long duration, TimeUnit unit, double variance )
    {
        this.duration = duration;
        this.unit = unit;
        this.variance = variance;
    }

    public double variance()
    {
        return variance;
    }

    public long to( TimeUnit unit )
    {
        return unit.convert( duration, this.unit );
    }

    public long to( TimeUnit unit, Random random )
    {
        long actual = to( unit );
        return actual + (long) ((random.nextDouble() * 2 * variance - variance) * actual);
    }
}
