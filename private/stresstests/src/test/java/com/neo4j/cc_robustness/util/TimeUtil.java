/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.TimeUtil.parseTimeMillis;

public class TimeUtil
{
    public static void sleepQuietly( long time )
    {
        try
        {
            Thread.sleep( time );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace( System.out );
            Thread.interrupted();
        }
    }

    public static Duration parseDuration( String timeWithOrWithoutUnit )
    {
        return new Duration( parseTimeMillis.apply( timeWithOrWithoutUnit ), MILLISECONDS );
    }

    public static Duration parseDuration( String timeWithOrWithoutUnit, double variance )
    {
        return new Duration( parseTimeMillis.apply( timeWithOrWithoutUnit ), MILLISECONDS, variance );
    }
}
