/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import com.neo4j.bench.data.SplittableRandomProvider;
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
        this.rng = SplittableRandomProvider.newRandom( threadParams.getThreadIndex() );
    }
}
