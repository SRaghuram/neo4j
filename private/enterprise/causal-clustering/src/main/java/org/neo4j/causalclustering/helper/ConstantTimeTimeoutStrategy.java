/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ConstantTimeTimeoutStrategy implements TimeoutStrategy
{
    private final Timeout constantTimeout;

    public ConstantTimeTimeoutStrategy( long backoffTime, TimeUnit timeUnit )
    {
        long backoffTimeMillis = timeUnit.toMillis( backoffTime );

        constantTimeout = new Timeout()
        {
            @Override
            public long getMillis()
            {
                return backoffTimeMillis;
            }

            @Override
            public void increment()
            {
            }
        };
    }

    public ConstantTimeTimeoutStrategy( Duration backoffTime )
    {
        this( backoffTime.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public Timeout newTimeout()
    {
        return constantTimeout;
    }
}
