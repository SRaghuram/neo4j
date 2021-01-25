/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.concurrent.TimeUnit;

/**
 * A fixed and constant timeout.
 */
public class FixedTimeout implements Timeout
{
    private final Delay delay;

    FixedTimeout( long amount, TimeUnit unit )
    {
        this.delay = new Delay( amount, unit );
    }

    @Override
    public Delay next()
    {
        return delay;
    }
}
