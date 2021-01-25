/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.QueryString;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class Runner
{
    private static final Logger LOG = LoggerFactory.getLogger( Runner.class );

    public void run( Jvm jvm,
                     Database database,
                     Map<Pid,Parameters> pidParameters,
                     Map<Pid,List<InternalProfiler>> pidProfilers,
                     QueryString warmupQueryString,
                     QueryString queryString,
                     BenchmarkGroup benchmarkGroup,
                     Benchmark benchmark,
                     ParametersReader parametersReader,
                     ForkDirectory forkDirectory,
                     MeasurementControl warmupControl,
                     MeasurementControl measurementControl,
                     boolean shouldRollbackWarmup,
                     MeasuringExecutor measuringExecutor ) throws Exception
    {
        // completely skip profiling and execution logic if there is no warmup for this query
        warmupControl.reset();
        if ( !warmupControl.isComplete() )
        {
            /*
             * Notify profilers that warmup is about to begin
             */
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onWarmupBegin( jvm,
                                                                                     forkDirectory,
                                                                                     pid,
                                                                                     getProfilerRecordingDescriptor( benchmarkGroup,
                                                                                                                     benchmark,
                                                                                                                     RunPhase.WARMUP,
                                                                                                                     profiler,
                                                                                                                     pidParameters.get( pid ) ) ) );
            }

            /*
             * Perform warmup
             */
            LOG.debug( format( "Performing warmup (%s). Policy: %s",
                               shouldRollbackWarmup ? warmupQueryString.executionMode() + " + ROLLBACK" : warmupQueryString.executionMode(),
                               warmupControl.description() ) );
            measuringExecutor.execute( warmupQueryString,
                                       parametersReader,
                                       warmupControl,
                                       database,
                                       forkDirectory,
                                       shouldRollbackWarmup,
                                       RunPhase.WARMUP );

            /*
             * Notify profilers that warmup has completed
             */
            for ( Pid pid : pidParameters.keySet() )
            {
                pidProfilers.get( pid ).forEach( profiler -> profiler.onWarmupFinished( jvm,
                                                                                        forkDirectory,
                                                                                        pid,
                                                                                        getProfilerRecordingDescriptor( benchmarkGroup,
                                                                                                                        benchmark,
                                                                                                                        RunPhase.WARMUP,
                                                                                                                        profiler,
                                                                                                                        pidParameters.get( pid ) ) ) );
            }
        }
        else
        {
            LOG.debug( format( "Skipping warmup. Policy: %s", warmupControl.description() ) );
        }

        /*
         * Notify profilers that measurement is about to begin
         */
        for ( Pid pid : pidParameters.keySet() )
        {
            pidProfilers.get( pid ).forEach( profiler -> profiler.onMeasurementBegin( jvm,
                                                                                      forkDirectory,
                                                                                      pid,
                                                                                      getProfilerRecordingDescriptor( benchmarkGroup,
                                                                                                                      benchmark,
                                                                                                                      RunPhase.MEASUREMENT,
                                                                                                                      profiler,
                                                                                                                      pidParameters.get( pid ) ) ) );
        }

        /*
         * Perform measurement
         */
        LOG.debug( format( "Performing measurement (%s). Policy: %s", queryString.executionMode(), measurementControl.description() ) );
        measuringExecutor.execute( queryString,
                                   parametersReader,
                                   measurementControl,
                                   database,
                                   forkDirectory,
                                   false,
                                   RunPhase.MEASUREMENT );

        /*
         * Notify profilers that measurement has completed
         */
        for ( Pid pid : pidParameters.keySet() )
        {
            pidProfilers.get( pid ).forEach( profiler -> profiler.onMeasurementFinished( jvm,
                                                                                         forkDirectory,
                                                                                         pid,
                                                                                         getProfilerRecordingDescriptor( benchmarkGroup,
                                                                                                                         benchmark,
                                                                                                                         RunPhase.MEASUREMENT,
                                                                                                                         profiler,
                                                                                                                         pidParameters.get( pid ) ) ) );
        }
    }

    private static ProfilerRecordingDescriptor getProfilerRecordingDescriptor( BenchmarkGroup benchmarkGroup,
                                                                               Benchmark benchmark,
                                                                               RunPhase runPhase,
                                                                               InternalProfiler profiler,
                                                                               Parameters additionalParameters )
    {
        return ProfilerRecordingDescriptor.create( benchmarkGroup,
                                                   benchmark,
                                                   runPhase,
                                                   ParameterizedProfiler.defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                   additionalParameters );
    }
}
