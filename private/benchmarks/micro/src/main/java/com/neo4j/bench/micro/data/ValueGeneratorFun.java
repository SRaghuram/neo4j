/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.util.SplittableRandom;

import org.neo4j.values.storable.Value;

public interface ValueGeneratorFun<T>
{
    /**
     * @return true if generator has exhausted its value space and started again from the beginning, false otherwise
     */
    boolean wrapped();

    /**
     * @return next value in sequence
     */
    T next( SplittableRandom rng );

    Value nextValue( SplittableRandom rng );
}
