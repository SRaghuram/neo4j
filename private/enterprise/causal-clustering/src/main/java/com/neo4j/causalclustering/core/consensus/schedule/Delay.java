/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.concurrent.TimeUnit;

public class Delay
{
    private final long amount;
    private final TimeUnit unit;

    Delay( long amount, TimeUnit unit )
    {

        this.amount = amount;
        this.unit = unit;
    }

    public long amount()
    {
        return amount;
    }

    public TimeUnit unit()
    {
        return unit;
    }
}
