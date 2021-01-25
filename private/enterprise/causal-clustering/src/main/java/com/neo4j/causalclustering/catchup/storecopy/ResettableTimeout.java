/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.time.Clock;
import java.time.Duration;

class ResettableTimeout implements ResettableCondition
{
    private final long timeout;
    private final Clock clock;
    private long absoluteTimeout = -1;

    ResettableTimeout( Duration timeout, Clock clock )
    {
        this.timeout = timeout.toMillis();
        this.clock = clock;
    }

    @Override
    public boolean canContinue()
    {
        if ( absoluteTimeout == -1 )
        {
            absoluteTimeout = clock.millis() + timeout;
            return true;
        }
        return absoluteTimeout > clock.millis();
    }

    @Override
    public void reset()
    {
        absoluteTimeout = -1;
    }
}
