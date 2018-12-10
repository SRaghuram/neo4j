/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.concurrent.TimeUnit;

public class TimeoutFactory
{
    public static Timeout fixedTimeout( long delay, TimeUnit unit )
    {
        return new FixedTimeout( delay, unit );
    }

    public static Timeout uniformRandomTimeout( long minDelay, long maxDelay, TimeUnit unit )
    {
        return new UniformRandomTimeout( minDelay, maxDelay, unit );
    }
}
