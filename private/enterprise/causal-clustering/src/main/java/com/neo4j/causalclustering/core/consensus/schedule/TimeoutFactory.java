/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class TimeoutFactory
{
    public static Timeout fixedTimeout( long delayInMillis, TimeUnit unit )
    {
        return new FixedTimeout( delayInMillis, unit );
    }

    public static Timeout uniformRandomTimeout( long minDelayInMillis, long maxDelayInMillis, TimeUnit unit )
    {
        return new UniformRandomTimeout( minDelayInMillis, maxDelayInMillis, unit );
    }

    public static MultiTimeout multiTimeout( Supplier<Enum> supplier )
    {
        return new MultiTimeout( supplier );
    }
}
