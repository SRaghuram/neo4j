/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfiler;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public abstract class InternalProfilerAssist
{
    private final Parameters ownParameters;

    public static InternalProfilerAssist forEmbedded( List<ProfilerType> profilerTypes )
    {
        Pid ownPid = HasPid.getPid();
        return new PidBasedInternalProfilerAssist( Parameters.NONE, singletonMap( ownPid, Parameters.NONE ), singletonMap( ownPid, profilerTypes ) );
    }

    public static InternalProfilerAssist forLocalServer( long neo4jPid,
                                                         List<ProfilerType> clientProfilerTypes,
                                                         List<ProfilerType> serverProfilerTypes )
    {
        Pid clientPid = HasPid.getPid();
        Pid serverPid = new Pid( neo4jPid );

        Map<Pid,Parameters> pidParameters = new HashMap<>();
        pidParameters.put( clientPid, Parameters.CLIENT );
        pidParameters.put( serverPid, Parameters.SERVER );

        Map<Pid,List<ProfilerType>> pidProfilers = new HashMap<>();
        pidProfilers.put( clientPid, clientProfilerTypes );
        pidProfilers.put( serverPid, serverProfilerTypes );

        return new PidBasedInternalProfilerAssist( Parameters.CLIENT, pidParameters, pidProfilers );
    }

    public abstract void onWarmupBegin( Jvm jvm,
                                        ForkDirectory forkDirectory,
                                        BenchmarkGroup benchmarkGroup,
                                        Benchmark benchmark );

    public abstract void onWarmupFinished( Jvm jvm,
                                           ForkDirectory forkDirectory,
                                           BenchmarkGroup benchmarkGroup,
                                           Benchmark benchmark );

    public abstract void onMeasurementBegin( Jvm jvm,
                                             ForkDirectory forkDirectory,
                                             BenchmarkGroup benchmarkGroup,
                                             Benchmark benchmark );

    public abstract void onMeasurementFinished( Jvm jvm,
                                                ForkDirectory forkDirectory,
                                                BenchmarkGroup benchmarkGroup,
                                                Benchmark benchmark );

    private InternalProfilerAssist( Parameters ownParameters )
    {
        this.ownParameters = ownParameters;
    }

    public Parameters ownParameter()
    {
        return ownParameters;
    }

    private static class PidBasedInternalProfilerAssist extends InternalProfilerAssist
    {
        private final Map<Pid,Parameters> pidParameters;
        private final Map<Pid,List<InternalProfiler>> pidProfilers;

        private PidBasedInternalProfilerAssist( Parameters ownParameters,
                                                Map<Pid,Parameters> pidParameters,
                                                Map<Pid,List<ProfilerType>> pidProfilers )
        {
            super( ownParameters );
            this.pidParameters = pidParameters;
            this.pidProfilers = new HashMap<>();
            assertInternal( pidProfilers );
            pidProfilers.keySet().forEach( pid -> this.pidProfilers.put( pid, ProfilerType.createInternalProfilers( pidProfilers.get( pid ) ) ) );
        }

        public void onWarmupBegin( Jvm jvm,
                                   ForkDirectory forkDirectory,
                                   BenchmarkGroup benchmarkGroup,
                                   Benchmark benchmark )
        {
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onWarmupBegin( jvm,
                                                                                     forkDirectory,
                                                                                     pid,
                                                                                     profilerRecordingDescriptor( benchmarkGroup,
                                                                                                                  benchmark,
                                                                                                                  RunPhase.WARMUP,
                                                                                                                  profiler,
                                                                                                                  pidParameters.get( pid ) ) ) );
            }
        }

        public void onWarmupFinished( Jvm jvm,
                                      ForkDirectory forkDirectory,
                                      BenchmarkGroup benchmarkGroup,
                                      Benchmark benchmark )
        {
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onWarmupFinished( jvm,
                                                                                        forkDirectory,
                                                                                        pid,
                                                                                        profilerRecordingDescriptor( benchmarkGroup,
                                                                                                                     benchmark,
                                                                                                                     RunPhase.WARMUP,
                                                                                                                     profiler,
                                                                                                                     pidParameters.get( pid ) ) ) );
            }
        }

        public void onMeasurementBegin( Jvm jvm,
                                        ForkDirectory forkDirectory,
                                        BenchmarkGroup benchmarkGroup,
                                        Benchmark benchmark )
        {
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onMeasurementBegin( jvm,
                                                                                          forkDirectory,
                                                                                          pid,
                                                                                          profilerRecordingDescriptor( benchmarkGroup,
                                                                                                                       benchmark,
                                                                                                                       RunPhase.MEASUREMENT,
                                                                                                                       profiler,
                                                                                                                       pidParameters.get( pid ) ) ) );
            }
        }

        public void onMeasurementFinished( Jvm jvm,
                                           ForkDirectory forkDirectory,
                                           BenchmarkGroup benchmarkGroup,
                                           Benchmark benchmark )
        {
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onMeasurementFinished( jvm,
                                                                                             forkDirectory,
                                                                                             pid,
                                                                                             profilerRecordingDescriptor( benchmarkGroup,
                                                                                                                          benchmark,
                                                                                                                          RunPhase.MEASUREMENT,
                                                                                                                          profiler,
                                                                                                                          pidParameters.get( pid ) ) ) );
            }
        }

        private static ProfilerRecordingDescriptor profilerRecordingDescriptor( BenchmarkGroup benchmarkGroup,
                                                                                Benchmark benchmark,
                                                                                RunPhase runPhase,
                                                                                InternalProfiler profiler,
                                                                                Parameters additionalParameters )
        {
            return ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                       benchmark,
                                                       runPhase,
                                                       defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                       additionalParameters );
        }
    }

    private static void assertInternal( Map<Pid,List<ProfilerType>> pidProfilers )
    {
        List<ProfilerType> allProfilerTypes = pidProfilers.keySet().stream()
                                                          .map( pidProfilers::get )
                                                          .flatMap( List::stream )
                                                          .distinct()
                                                          .collect( toList() );
        ProfilerType.assertInternal( allProfilerTypes );
    }
}
