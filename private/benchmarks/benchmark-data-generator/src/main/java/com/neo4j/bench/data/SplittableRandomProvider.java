/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.util.SplittableRandom;

public class SplittableRandomProvider
{
    public static SplittableRandom newRandom( long seed )
    {
        return new SplittableRandom( seed );
    }
}
