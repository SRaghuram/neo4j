/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.SplittableRandom;

@State( Scope.Thread )
public class RNGState
{
    public SplittableRandom rng;

    @Setup
    public void setUp( ThreadParams threadParams )
    {
        this.rng = newRandom( threadParams );
    }

    public static SplittableRandom newRandom( ThreadParams threadParams )
    {
        // ensures that every thread gets a different seed
        return newRandom( threadParams.getThreadIndex() );
    }

    public static SplittableRandom newRandom( long seed )
    {
        return new SplittableRandom( seed );
    }
}
