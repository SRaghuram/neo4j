/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.quick;

import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * "I just wanna quickly JMH-benchmark this thing w/o leaving the IDE or do any sort of mvn install commands"
 *
 * Here's what you do:
 *
 * <ol>
 *     <li>Create a benchmark class, you know with one or more @Benchmark benchmark methods in it - in this very module.
 *     Rather create it in the same package as the thing you're benchmarking, this will give you access to package-private things too</li>
 *     <li>Include a main-method in your benchmark class looking something like:
 *     <pre>
 *     public static void main( String[] args ) throws RunnerException
 *     {
 *         QuickBenchmark.benchmark().include( MyBenchmark.class ).run();
 *     }
 *     </pre>
 *     </li>
 *     <li>Run your benchmark class, i.e. Ctrl+F9 or similar</li>
 *     <li>If JMH complains about no benchmark found then
 *     <li>Ensure annotation processing is enabled for the 'quick benchmarks' module, i.e. the module this class lives in</li>
 *     <li>Hit Ctrl+Shift+F9, i.e. rebuild the "quick benchmarks" module.</li>
 *     <li>(And then run again).</li>
 *     Forcing rebuild of this module will guarantee that the JMH annotation processor finds your new benchmark and adds it to its "BenchmarkList" file
 *     that it needs in order to run it</li>
 * </ol>
 *
 * All JMH options are available to modify using {@link #option(Consumer)}. The {@link #include(Class)} methods are there for added convenience.
 */
public class QuickBenchmark
{
    private final OptionsBuilder options = new OptionsBuilder();

    private QuickBenchmark()
    {
    }

    /**
     * Default options for everything. Modify options with either {@link #include(Class)} methods or {@link #option(Consumer)}.
     */
    public static QuickBenchmark benchmark()
    {
        return new QuickBenchmark();
    }

    public QuickBenchmark include( Class<?> cls )
    {
        return option( opts -> opts.include( cls.getSimpleName() ) );
    }

    public QuickBenchmark include( Class<?> cls, String... methods )
    {
        return option( opts -> Stream.of( methods ).forEach( method -> opts.include( cls.getSimpleName() + "." + method ) ) );
    }

    public QuickBenchmark forks( int forks )
    {
        return option( opts -> opts.forks( forks ) );
    }

    public QuickBenchmark iterations( int warmupIterations, int measureIterations )
    {
        return option( opts ->
        {
            opts.warmupIterations( warmupIterations );
            opts.measurementIterations( measureIterations );
        } );
    }

    public QuickBenchmark option( Consumer<OptionsBuilder> modifier )
    {
        modifier.accept( options );
        return this;
    }

    public void run() throws RunnerException
    {
        org.openjdk.jmh.runner.Runner runner = new org.openjdk.jmh.runner.Runner( options.build() );
        runner.run();
    }
}
