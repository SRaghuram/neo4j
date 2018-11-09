/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.metric;

import com.codahale.metrics.Meter;

import java.util.function.LongSupplier;

public class MetricsCounter extends Meter
{
    private final LongSupplier countSupplier;

    public MetricsCounter( LongSupplier countSupplier )
    {
        this.countSupplier = countSupplier;
    }

    @Override
    public long getCount()
    {
        return countSupplier.getAsLong();
    }
}
