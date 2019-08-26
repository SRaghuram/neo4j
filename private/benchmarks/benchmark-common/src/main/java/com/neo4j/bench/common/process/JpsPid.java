/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.Jvm;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.neo4j.bench.common.util.BenchmarkUtil.inputStreamToString;
import static com.neo4j.bench.common.util.BenchmarkUtil.lessWhiteSpace;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.stream.Collectors.toList;

public class JpsPid
{
    public static JpsPid tryFindFor( Jvm jvm, Instant start, Duration timeout, String processName )
    {
        try
        {
            while ( true )
            {
                JpsPid jpsPid = processPid( jvm, processName );
                if ( jpsPid.found() || hasTimedOut( start, timeout ) )
                {
                    return jpsPid;
                }
                Thread.sleep( Duration.ofSeconds( 1 ).toMillis() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving PID for process: " + processName, e );
        }
    }

    private static JpsPid processPid( Jvm jvm, String processName ) throws IOException, InterruptedException
    {
        String[] command = {jvm.launchJps(), "-v"};
        Process jpsProcess = new ProcessBuilder( command ).start();
        int resultCode = jpsProcess.waitFor();
        String jpsOutput = inputStreamToString( jpsProcess.getInputStream() );
        if ( resultCode != 0 )
        {
            System.out.println( "Bad things happened when invoking 'jps', trying pgrep and ps before exiting\n" +
                                "Code   : " + resultCode + "\n" +
                                "Output : " + jpsOutput );
            return tryPgrep( processName );
        }

        return fromProcessOutput( jpsOutput, processName );
    }

    static JpsPid tryPgrep( String processName ) throws IOException, InterruptedException
    {
        String[] command = {"bash", "-c", "pgrep java | xargs ps -p"};
        ProcessBuilder processBuilder = new ProcessBuilder( command );
        Process psProcess = processBuilder.start();
        int resultCode = psProcess.waitFor();
        String psOutput = inputStreamToString( psProcess.getInputStream() );
        if ( resultCode != 0 )
        {
            throw new RuntimeException( "Bad things happened when invoking 'ps'\n" +
                                        "Code   : " + resultCode + "\n" +
                                        "Output : " + psOutput );
        }
        return fromProcessOutput( psOutput, processName );
    }

    private static JpsPid fromProcessOutput( String jpsOutput, String processName )
    {
        long pid = jpsOutput.contains( processName )
                   ? pidFromJpsOutput( jpsOutput, processName )
                   : -1;
        return new JpsPid( pid, jpsOutput );
    }

    private static boolean hasTimedOut( Instant start, Duration warmupTimeout )
    {
        Duration durationWaited = between( start, now() );
        return durationWaited.compareTo( warmupTimeout ) >= 0;
    }

    private final long pid;
    private final String jpsOutput;

    private JpsPid( long pid, String jpsOutput )
    {
        this.pid = pid;
        this.jpsOutput = jpsOutput;
    }

    private boolean found()
    {
        return -1 != pid;
    }

    public Optional<Long> pid()
    {
        return found() ? Optional.of( pid ) : Optional.empty();
    }

    public String jpsOutput()
    {
        return jpsOutput;
    }

    private static long pidFromJpsOutput( String jpsOutput, String processName )
    {
        String sanitizedJpsOutput = lessWhiteSpace( jpsOutput );
        int indexOfProcessName = sanitizedJpsOutput.indexOf( "-Dname=" + processName );
        // remove anything that appears after the process name we want
        String[] jpsOutputTokens = sanitizedJpsOutput.substring( 0, indexOfProcessName ).split( " " );
        // take the last number token, the one that appears just before our process name
        List<Long> pids = Arrays.stream( jpsOutputTokens )
                                .filter( JpsPid::isPid )
                                .map( Long::parseLong )
                                .collect( toList() );
        return pids.get( pids.size() - 1 );
    }

    private static boolean isPid( String value )
    {
        try
        {
            return Long.parseLong( value ) >= 0;
        }
        catch ( NumberFormatException e )
        {
            return false;
        }
    }
}
