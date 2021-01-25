/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

public class TestUtils
{
    public static final double LOWER_PERCENT = 0.9;
    public static final double UPPER_PERCENT = 1.1;
    public static final long DIFFERENCE_ABSOLUTE = 50;

    public static long operationCountLower( long operationCount )
    {
        return Math.min(
                percent( operationCount, LOWER_PERCENT ),
                operationCount - DIFFERENCE_ABSOLUTE
        );
    }

    public static long operationCountUpper( long operationCount )
    {
        return Math.max(
                percent( operationCount, UPPER_PERCENT ),
                operationCount + DIFFERENCE_ABSOLUTE
        );
    }

    public static long percent( long value, double percent )
    {
        return Math.round( value * percent );
    }
}
