/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.profiling;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.client.ResultsDirectory;
import com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.temporal.TemporalUtil;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.profiling.ExternalProfiler;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.Jvm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.INITIALIZING;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.MEASUREMENT;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.MEASUREMENT_FINISHED;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.WARMUP;
import static com.ldbc.driver.client.ResultsDirectory.BenchmarkPhase.WARMUP_FINISHED;
import static com.ldbc.driver.util.FileUtils.inputStringToString;
import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;
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
            Optional<Integer> maybePid = processPid( start, Duration.of( 1, MINUTES ), processName );
            if ( !maybePid.isPresent() )
            {
                throw new RuntimeException( "Process '" + processName + "' not found!!" );
            }

            waitForWarmupToBegin( start, forkDirectory, processName );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupBegin..." );
                                           internalProfiler.onWarmupBegin( jvm, forkDirectory, maybePid.get(), benchmarkGroup, benchmark );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupBegin - DONE" );
                                       } );

            waitForWarmupToComplete( start, forkDirectory, processName );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupFinished..." );
                                           internalProfiler.onWarmupFinished( jvm, forkDirectory, maybePid.get(), benchmarkGroup, benchmark );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onWarmupFinished - DONE" );
                                       } );

            waitForMeasurementToBegin( start, forkDirectory, processName );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementBegin..." );
                                           internalProfiler.onMeasurementBegin( jvm, forkDirectory, maybePid.get(), benchmarkGroup, benchmark );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementBegin - DONE" );
                                       } );

            waitForMeasurementToComplete( start, forkDirectory, processName );
            // Race condition here
            assertProcessAlive( process );
            internalProfilers.forEach( internalProfiler ->
                                       {
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementFinished..." );
                                           internalProfiler.onMeasurementFinished( jvm, forkDirectory, maybePid.get(), benchmarkGroup, benchmark );
                                           System.out.println( internalProfiler.getClass().getSimpleName() + ".onMeasurementFinished - DONE" );
                                       } );
            // Race condition here
            assertProcessAlive( process );
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
                                       externalProfiler.beforeProcess( forkDirectory, benchmarkGroup, benchmark );
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
                                       externalProfiler.afterProcess( forkDirectory, benchmarkGroup, benchmark );
                                       System.out.println( "After Process (FINISH): " + externalProfiler.description() );
                                   } );
    }

    private static void waitForWarmupToBegin(
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                start,
                forkDirectory,
                processName,
                newHashSet( INITIALIZING, WARMUP ),
                newHashSet( WARMUP, WARMUP_FINISHED, MEASUREMENT, MEASUREMENT_FINISHED ) );
    }

    private static void waitForWarmupToComplete(
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                start,
                forkDirectory,
                processName,
                newHashSet( WARMUP, WARMUP_FINISHED ),
                newHashSet( WARMUP_FINISHED, MEASUREMENT, MEASUREMENT_FINISHED ) );
    }

    private static void waitForMeasurementToBegin(
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                start,
                forkDirectory,
                processName,
                newHashSet( WARMUP_FINISHED, MEASUREMENT ),
                newHashSet( MEASUREMENT, MEASUREMENT_FINISHED ) );
    }

    private static void waitForMeasurementToComplete(
            Instant start,
            ForkDirectory forkDirectory,
            String processName ) throws ClientException, IOException, DriverConfigurationException, InterruptedException
    {
        waitFor(
                start,
                forkDirectory,
                processName,
                newHashSet( MEASUREMENT, MEASUREMENT_FINISHED ),
                newHashSet( MEASUREMENT_FINISHED ) );
    }

    private static void waitFor(
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
            start = checkTimeoutAndMaybeReset( start, processName );
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

    private static Instant checkTimeoutAndMaybeReset( Instant start, String processName )
    {
        Duration durationWaited = between( start, now() );
        if ( durationWaited.compareTo( CHECK_ALIVE_TIMEOUT ) >= 0 )
        {
            // If process is still alive, reset start time and wait another timeout
            boolean processIsAlive = processPid( now(), Duration.of( 10, SECONDS ), processName ).isPresent();
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

    private static Optional<Integer> processPid( Instant start, Duration timeout, String processName )
    {
        try
        {
            while ( true )
            {
                Optional<Integer> maybePid = processPid( processName );
                if ( maybePid.isPresent() )
                {
                    return maybePid;
                }
                checkTimeout( start, timeout );
                Thread.sleep( 1000 );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving PID for process: " + processName, e );
        }
    }

    private static Optional<Integer> processPid( String processName ) throws IOException, InterruptedException
    {
        String[] command = {"jps", "-v"};
        Process process = new ProcessBuilder( command ).start();
        int resultCode = process.waitFor();
        if ( resultCode != 0 )
        {
            throw new RuntimeException( "Bad things happened when invoking JPS. Code: " + resultCode );
        }
        String processOutput = inputStringToString( process.getInputStream() );
        return (processOutput.contains( processName ))
               ? Optional.of( pidFromJpsOutput( processOutput, processName ) )
               : Optional.empty();
    }

    private static int pidFromJpsOutput( String jpsOutput, String processName )
    {
        String sanitizedJpsOutput = BenchmarkUtil.lessWhiteSpace( jpsOutput );
        int indexOfProcessName = sanitizedJpsOutput.indexOf( "-Dname=" + processName );
        // remove anything that appears after the process name we want
        String[] jpsOutputTokens = sanitizedJpsOutput.substring( 0, indexOfProcessName ).split( " " );
        // take the last number token, the one that appears just before our process name
        List<Integer> pids = Arrays.stream( jpsOutputTokens )
                                   .filter( ProfilerRunner::isPid )
                                   .map( Integer::parseInt )
                                   .collect( toList() );
        return pids.get( pids.size() - 1 );
    }

    private static boolean isPid( String value )
    {
        try
        {
            return Integer.parseInt( value ) >= 0;
        }
        catch ( NumberFormatException e )
        {
            return false;
        }
    }
}
