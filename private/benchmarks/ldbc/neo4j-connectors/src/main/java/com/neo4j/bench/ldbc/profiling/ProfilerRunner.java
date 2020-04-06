/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.profiling;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.client.ResultsDirectory;
import com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.temporal.TemporalUtil;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.common.process.JpsPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.INITIALIZING;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.MEASUREMENT;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.MEASUREMENT_FINISHED;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.WARMUP;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.WARMUP_FINISHED;
import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ProfilerRunner
{
    private static final Duration CHECK_ALIVE_TIMEOUT = Duration.of( 5, MINUTES );

    public static void profile(
            Jvm jvm,
            Instant start,
            Process process,
            String processName,
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            List<InternalProfiler> internalProfilers )
    {
        try
        {
            // If process is still alive, reset start time and wait another timeout
            JpsPid jpsPid = new JpsPid();
            jpsPid.tryFindFor( jvm, now(), Duration.of( 1, MINUTES ), processName );
            if ( !jpsPid.pid().isPresent() )
            {
                throw new RuntimeException( "Process '" + processName + "' not found!!" );
            }
            Pid pid = new Pid( jpsPid.pid().get() );

            waitForWarmupToBegin( jvm, start, forkDirectory, processName );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupBegin..." );
                                           internalProfiler.onWarmupBegin( jvm,
                                                                           forkDirectory,
                                                                           pid,
                                                                           benchmarkGroup,
                                                                           benchmark,
                                                                           Parameters.NONE );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupBegin - DONE" );
                                       } );

            waitForWarmupToComplete( jvm, start, forkDirectory, processName );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupFinished..." );
                                           internalProfiler.onWarmupFinished( jvm,
                                                                              forkDirectory,
                                                                              pid,
                                                                              benchmarkGroup,
                                                                              benchmark,
                                                                              Parameters.NONE );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupFinished - DONE" );
                                       } );

            waitForMeasurementToBegin( jvm, start, forkDirectory, processName );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementBegin..." );
                                           internalProfiler.onMeasurementBegin( jvm,
                                                                                forkDirectory,
                                                                                pid,
                                                                                benchmarkGroup,
                                                                                benchmark,
                                                                                Parameters.NONE );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementBegin - DONE" );
                                       } );

            waitForMeasurementToComplete( jvm, start, forkDirectory, processName );
            // Race condition here
            assertProcessAlive( process );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementFinished..." );
                                           internalProfiler.onMeasurementFinished( jvm,
                                                                                   forkDirectory,
                                                                                   pid,
                                                                                   benchmarkGroup,
                                                                                   benchmark,
                                                                                   Parameters.NONE );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementFinished - DONE" );
                                       } );
            // Race condition here
            assertProcessAlive( process );

            for ( InternalProfiler internalProfiler : internalProfilers )
            {
                ProfilerType profilerType = ProfilerType.typeOf( internalProfiler );
                if ( !profilerType.isExternal() )
                {
                    // profiler recording cleanup should only happen once
                    // if profiler is both internal and external, cleanup will happen in afterTrial()
                    forkDirectory.unsanitizeProfilerRecordingsFor( benchmarkGroup, benchmark, profilerType, Parameters.NONE );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error profiling LDBC fork", e );
        }
    }

    public static void beforeProcess(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            List<ExternalProfiler> externalProfilers )
    {
        externalProfilers.forEach( externalProfiler ->
                                   {
                                       System.out.println( "Before Process (START):  " + externalProfiler.description() );
                                       externalProfiler.beforeProcess( forkDirectory, benchmarkGroup, benchmark, Parameters.NONE );
                                       System.out.println( "Before Process (FINISH): " + externalProfiler.description() );
                                   } );
    }

    public static void afterProcess(
            ForkDirectory forkDirectory,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            List<ExternalProfiler> externalProfilers )
    {
        externalProfilers.forEach( externalProfiler ->
                                   {
                                       System.out.println( "After Process (START):  " + externalProfiler.description() );
                                       externalProfiler.afterProcess( forkDirectory, benchmarkGroup, benchmark, Parameters.NONE );
                                       System.out.println( "After Process (FINISH): " + externalProfiler.description() );
                                   } );

        for ( ExternalProfiler externalProfiler : externalProfilers )
        {
            ProfilerType profilerType = ProfilerType.typeOf( externalProfiler );
            forkDirectory.unsanitizeProfilerRecordingsFor( benchmarkGroup, benchmark, profilerType, Parameters.NONE );
        }
    }

    private static void waitForWarmupToBegin(
            Jvm jvm,
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                jvm,
                start,
                forkDirectory,
                processName,
                newHashSet( INITIALIZING, WARMUP ),
                newHashSet( WARMUP, WARMUP_FINISHED, MEASUREMENT, MEASUREMENT_FINISHED ) );
    }

    private static void waitForWarmupToComplete(
            Jvm jvm,
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                jvm,
                start,
                forkDirectory,
                processName,
                newHashSet( WARMUP, WARMUP_FINISHED ),
                newHashSet( WARMUP_FINISHED, MEASUREMENT, MEASUREMENT_FINISHED ) );
    }

    private static void waitForMeasurementToBegin(
            Jvm jvm,
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                jvm,
                start,
                forkDirectory,
                processName,
                newHashSet( WARMUP_FINISHED, MEASUREMENT ),
                newHashSet( MEASUREMENT, MEASUREMENT_FINISHED ) );
    }

    private static void waitForMeasurementToComplete(
            Jvm jvm,
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                jvm,
                start,
                forkDirectory,
                processName,
                newHashSet( MEASUREMENT, MEASUREMENT_FINISHED ),
                newHashSet( MEASUREMENT_FINISHED ) );
    }

    private static void waitFor(
            Jvm jvm,
            Instant start,
            ForkDirectory forkDirectory,
            String processName,
            Set<BenchmarkPhase> validStartPhases,
            Set<BenchmarkPhase> validEndPhases )
            throws InterruptedException, ClientException, DriverConfigurationException, IOException
    {
        File resultsDir = Paths.get( forkDirectory.toAbsolutePath() ).toFile();
        BenchmarkPhase phase = ResultsDirectory.phase( resultsDir );
        assertPhase( phase, validStartPhases.toArray( new BenchmarkPhase[0] ) );
        while ( !validEndPhases.contains( phase ) )
        {
            Thread.sleep( 1000 );
            start = checkTimeoutAndMaybeReset( jvm, start, processName );
            phase = ResultsDirectory.phase( resultsDir );
        }
        System.out.println( soFar( start ) + " - Fork status: " + phase );
    }

    private static void assertProcessAlive( Process process )
    {
        if ( !process.isAlive() )
        {
            throw new RuntimeException( "LDBC fork process completed too fast" );
        }
    }

    private static void assertPhase( BenchmarkPhase actualPhase, BenchmarkPhase... expectedPhases )
    {
        Set<BenchmarkPhase> expectedPhaseSet = Arrays.stream( expectedPhases ).collect( toSet() );
        if ( !expectedPhaseSet.contains( actualPhase ) )
        {
            throw new RuntimeException( format( "Expected phases %s but was %s", expectedPhaseSet, actualPhase ) );
        }
    }

    public static void checkTimeout( Instant start, Duration warmupTimeout )
    {
        Duration durationWaited = between( start, now() );
        if ( durationWaited.compareTo( warmupTimeout ) >= 0 )
        {
            throw new RuntimeException( "Grew tired of waiting after " + durationString( durationWaited ) );
        }
    }

    private static Instant checkTimeoutAndMaybeReset( Jvm jvm, Instant start, String processName )
    {
        Duration durationWaited = between( start, now() );
        if ( durationWaited.compareTo( CHECK_ALIVE_TIMEOUT ) >= 0 )
        {
            // If process is still alive, reset start time and wait another timeout
            JpsPid jpsPid = new JpsPid();
            jpsPid.tryFindFor( jvm, now(), Duration.of( 10, SECONDS ), processName );
            boolean processIsAlive = jpsPid.pid().isPresent();
            if ( processIsAlive )
            {
                System.out.println( "Process `" + processName + "` is still alive" );
                return now();
            }
            else
            {
                throw new RuntimeException(
                        "Was waiting on process '" + processName + "', but it seems to have died. RIP.\n" +
                        "Have been waiting for: " + durationString( durationWaited ) );
            }
        }
        else
        {
            return start;
        }
    }

    private static String soFar( Instant start )
    {
        return durationString( between( start, now() ) );
    }

    private static String durationString( Duration duration )
    {
        return new TemporalUtil().milliDurationToString( duration.toMillis() );
    }
}
