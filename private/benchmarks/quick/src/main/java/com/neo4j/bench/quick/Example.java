/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.quick;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State( Scope.Benchmark )
public class Example
{
    @Param( "my default value" )
    public String myFirstParam;

    @Param( {"my", "default", "value"} )
    public String mySecondParam;

    @Setup
    public void setUpBenchmark()
    {
    }

    @TearDown
    public void tearDownBenchmark()
    {
    }

    @State( Scope.Thread )
    public static class StateClass
    {
        int myState;

        @Setup
        public void setUp( Example benchmark ) throws InterruptedException
        {
            myState = Integer.parseInt( benchmark.myFirstParam );
        }

        @TearDown
        public void tearDown()
        {
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void myFirstBenchmark( StateClass state )
    {
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public void mySecondBenchmark( StateClass state )
    {
    }
}
