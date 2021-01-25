/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.Jvm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class PidStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger( PidStrategy.class );

    public PidStrategy tryFindFor( Jvm jvm, Instant start, Duration timeout, String processName )
    {
        try
        {
            while ( true )
            {
                PidStrategy pidStrategy = processPid( jvm, processName );
                if ( pidStrategy.found() || hasTimedOut( start, timeout ) )
                {
                    return pidStrategy;
                }
                Thread.sleep( Duration.ofSeconds( 1 ).toMillis() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving PID for process: " + processName, e );
        }
    }

    private PidStrategy processPid( Jvm jvm, String processName ) throws IOException, InterruptedException
    {
        String[] command = getCommand( jvm );
        Process pidFinderProcess = new ProcessBuilder( command ).start();
        int resultCode = pidFinderProcess.waitFor();
        String output = inputStreamToString( pidFinderProcess.getInputStream() );
        if ( resultCode != 0 )
        {
            LOG.debug( "Bad things happened when invoking '" + this.getClass() + "'\n" +
                       "Code   : " + resultCode + "\n" +
                       "Output : " + output );
        }

        return fromProcessOutput( output, processName );
    }

    /**
     * Should return the correct comand for the Pid finder
     *
     * @return the correct command
     */
    protected abstract String[] getCommand( Jvm jvm );

    protected PidStrategy fromProcessOutput( String output, String processName )
    {
        this.pid = output.contains( processName )
                   ? pidFromOutput( output, processName )
                   : -1;
        this.output = output;
        return this;
    }

    private static boolean hasTimedOut( Instant start, Duration warmupTimeout )
    {
        Duration durationWaited = between( start, now() );
        return durationWaited.compareTo( warmupTimeout ) >= 0;
    }

    private long pid;
    private String output;

    PidStrategy()
    {
    }

    private boolean found()
    {
        return -1 != pid;
    }

    public Optional<Long> pid()
    {
        return found() ? Optional.of( pid ) : Optional.empty();
    }

    public String output()
    {
        return output;
    }

    private static long pidFromOutput( String output, String processName )
    {
        String sanitizedOutput = lessWhiteSpace( output );
        int indexOfProcessName = sanitizedOutput.indexOf( "-Dname=" + processName );
        // remove anything that appears after the process name we want
        String[] outputTokens = sanitizedOutput.substring( 0, indexOfProcessName ).split( " " );
        // take the last number token, the one that appears just before our process name
        List<Long> pids = Arrays.stream( outputTokens )
                                .filter( PidStrategy::isPid )
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

    @Override
    public String toString()
    {
        return "PidStrategy{" +
               "pid=" + pid +
               ", output='" + output + '\'' +
               '}';
    }
}
