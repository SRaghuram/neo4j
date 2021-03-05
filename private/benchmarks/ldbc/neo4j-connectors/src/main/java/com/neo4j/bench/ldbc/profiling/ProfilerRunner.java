/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.profiling;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.client.ResultsDirectory;
import com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.temporal.TemporalUtil;
import com.neo4j.bench.common.process.JpsPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.assist.InternalProfilerAssist;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
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
    private static final Logger LOG = LoggerFactory.getLogger( ProfilerRunner.class );
    private static final Duration CHECK_ALIVE_TIMEOUT = Duration.of( 5, MINUTES );

    public static void profile(
            Jvm jvm,
            Instant start,
            Process process,
            String processName,
            ForkDirectory forkDirectory,
            InternalProfilerAssist assist ) throws ProfilerRunnerException
    {
        try
        {
            // If process is still alive, reset start time and wait another timeout
            waitForWarmupToBegin( jvm, start, forkDirectory, processName );
            assist.onWarmupBegin();

            waitForWarmupToComplete( jvm, start, forkDirectory, processName );
            assist.onWarmupFinished();

            waitForMeasurementToBegin( jvm, start, forkDirectory, processName );
            assist.onMeasurementBegin();

            waitForMeasurementToComplete( jvm, start, forkDirectory, processName );
            // Race condition here
            assertProcessAlive( process );
            assist.onMeasurementFinished();
            // Race condition here
            assertProcessAlive( process );
        }
        catch ( Exception e )
        {
            throw new ProfilerRunnerException( "Error profiling LDBC fork", e );
        }
    }

    public static Pid getPid( Jvm jvm, String processName ) throws ProfilerRunnerException
    {
        JpsPid jpsPid = new JpsPid();
        jpsPid.tryFindFor( jvm, now(), Duration.of( 1, MINUTES ), processName );
        if ( !jpsPid.pid().isPresent() )
        {
            throw new ProfilerRunnerException( "Process '" + processName + "' not found!!" );
        }
        return new Pid( jpsPid.pid().get() );
    }

    private static void waitForWarmupToBegin(
            Jvm jvm,
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException, ProfilerRunnerException
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
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException, ProfilerRunnerException
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
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException, ProfilerRunnerException
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
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException, ProfilerRunnerException
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
            throws InterruptedException, ClientException, DriverConfigurationException, IOException, ProfilerRunnerException
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
        LOG.debug( soFar( start ) + " - Fork status: " + phase );
    }

    private static void assertProcessAlive( Process process ) throws ProfilerRunnerException
    {
        if ( !process.isAlive() )
        {
            throw new ProfilerRunnerException( "LDBC fork process completed too fast" );
        }
    }

    private static void assertPhase( BenchmarkPhase actualPhase, BenchmarkPhase... expectedPhases ) throws ProfilerRunnerException
    {
        Set<BenchmarkPhase> expectedPhaseSet = Arrays.stream( expectedPhases ).collect( toSet() );
        if ( !expectedPhaseSet.contains( actualPhase ) )
        {
            throw new ProfilerRunnerException( format( "Expected phases %s but was %s", expectedPhaseSet, actualPhase ) );
        }
    }

    public static void checkTimeout( Instant start, Duration warmupTimeout ) throws ProfilerRunnerException
    {
        Duration durationWaited = between( start, now() );
        if ( durationWaited.compareTo( warmupTimeout ) >= 0 )
        {
            throw new ProfilerRunnerException( "Grew tired of waiting after " + durationString( durationWaited ) );
        }
    }

    private static Instant checkTimeoutAndMaybeReset( Jvm jvm, Instant start, String processName ) throws ProfilerRunnerException
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
                LOG.debug( "Process `" + processName + "` is still alive" );
                return now();
            }
            else
            {
                throw new ProfilerRunnerException(
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
