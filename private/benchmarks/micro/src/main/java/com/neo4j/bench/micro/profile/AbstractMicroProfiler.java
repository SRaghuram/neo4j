/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.profile;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.process.HasPid;
import com.neo4j.bench.client.profiling.Profiler;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.JvmVersion;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

import static com.neo4j.bench.micro.JMHResultUtil.toBenchmarkGroup;
import static com.neo4j.bench.micro.JMHResultUtil.toBenchmarks;
import static com.neo4j.bench.micro.config.JmhOptionsUtil.getStores;

import static java.util.Collections.singletonList;

abstract class AbstractMicroProfiler implements InternalProfiler, ExternalProfiler
{
    private ProfilerType profilerType;
    private Profiler innerProfiler;
    private ForkDirectory forkDir;

    private int iteration;

    AbstractMicroProfiler()
    {
        super();
        profilerType = profilerType();
        innerProfiler = profilerType.create();
    }

    abstract ProfilerType profilerType();

    private ForkDirectory getForkDir( BenchmarkParams params, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        if ( null == forkDir )
        {
            forkDir = getStores( params ).newForkDirectoryFor( benchmarkGroup, benchmark, singletonList( profilerType ) );
        }
        return forkDir;
    }

    @Override
    public Collection<String> addJVMInvokeOptions( BenchmarkParams params )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = toBenchmarks( params ).parentBenchmark();
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            return ((com.neo4j.bench.client.profiling.ExternalProfiler) innerProfiler).jvmInvokeArgs( forkDir, benchmarkGroup, benchmark );
        }
        else
        {
            return Collections.emptyList();
        }
    }

    @Override
    public Collection<String> addJVMOptions( BenchmarkParams params )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = toBenchmarks( params ).parentBenchmark();
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
            JvmVersion jvmVersion = jvm.version();
            return ((com.neo4j.bench.client.profiling.ExternalProfiler) innerProfiler).jvmArgs( jvmVersion, forkDir, benchmarkGroup, benchmark );
        }
        else
        {
            return Collections.emptyList();
        }
    }

    @Override
    public void beforeTrial( BenchmarkParams params )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = toBenchmarks( params ).parentBenchmark();
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            ((com.neo4j.bench.client.profiling.ExternalProfiler) innerProfiler).beforeProcess( forkDir, benchmarkGroup, benchmark );
        }
    }

    @Override
    public Collection<? extends Result> afterTrial( BenchmarkResult benchmarkResult, long pid, File stdOut, File stdErr )
    {
        if ( profilerType.isExternal() )
        {
            BenchmarkParams params = benchmarkResult.getParams();
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = toBenchmarks( params ).parentBenchmark();
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            ((com.neo4j.bench.client.profiling.ExternalProfiler) innerProfiler).afterProcess( forkDir, benchmarkGroup, benchmark );
        }
        return Collections.emptyList();
    }

    @Override
    public boolean allowPrintOut()
    {
        return true;
    }

    @Override
    public boolean allowPrintErr()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return innerProfiler.description();
    }

    @Override
    public void beforeIteration( BenchmarkParams params, IterationParams iterationParams )
    {
        // only start profiling on first iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && ++iteration == 1 )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = toBenchmarks( params ).parentBenchmark();
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
            ((com.neo4j.bench.client.profiling.InternalProfiler) innerProfiler).onMeasurementBegin( jvm,
                                                                                                    forkDir,
                                                                                                    HasPid.getPid(),
                                                                                                    benchmarkGroup,
                                                                                                    benchmark );
        }
    }

    @Override
    public Collection<? extends Result> afterIteration( BenchmarkParams params, IterationParams iterationParams, IterationResult iterationResult )
    {
        // only stop profiling on last iteration
        if ( profilerType.isInternal() && iterationParams.getType() == IterationType.MEASUREMENT && iteration == iterationParams.getCount() )
        {
            BenchmarkGroup benchmarkGroup = toBenchmarkGroup( params );
            Benchmark benchmark = toBenchmarks( params ).parentBenchmark();
            ForkDirectory forkDir = getForkDir( params, benchmarkGroup, benchmark );
            Jvm jvm = Jvm.bestEffortOrFail( Paths.get( params.getJvm() ) );
            ((com.neo4j.bench.client.profiling.InternalProfiler) innerProfiler).onMeasurementFinished( jvm,
                                                                                                       forkDir,
                                                                                                       HasPid.getPid(),
                                                                                                       benchmarkGroup,
                                                                                                       benchmark );
        }
        return Collections.emptyList();
    }
}
