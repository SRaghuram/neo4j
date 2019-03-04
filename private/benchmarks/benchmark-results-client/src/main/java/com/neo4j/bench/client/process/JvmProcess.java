/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.process;

import com.neo4j.bench.client.util.Jvm;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.neo4j.bench.client.util.BenchmarkUtil.inputStreamToString;
import static com.neo4j.bench.client.util.BenchmarkUtil.lessWhiteSpace;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.stream.Collectors.toList;

public class JvmProcess implements BaseProcess, HasPid
{
    public static JvmProcess start( JvmProcessArgs jvmProcessArgs )
    {
        return start( jvmProcessArgs, Redirect.INHERIT, Redirect.INHERIT );
    }

    public static JvmProcess start( JvmProcessArgs jvmProcessArgs, Redirect outputRedirect, Redirect errorRedirect )
    {
        try
        {
            ProcessBuilder processBuilder = new ProcessBuilder()
                    .command( jvmProcessArgs.args() )
                    .redirectOutput( outputRedirect )
                    .redirectError( errorRedirect );
            ProcessWrapper process = ProcessWrapper.start( processBuilder );
            Instant start = Instant.now();
            Duration timeout = Duration.of( 5, ChronoUnit.MINUTES );
            JpsPid jpsPid = processPid( jvmProcessArgs.jvm(), start, timeout, jvmProcessArgs.processName() );
            long pid = jpsPid.pid().orElseThrow( () -> failedToStartExceptionFor( jvmProcessArgs, process, jpsPid ) );
            return new JvmProcess( process, pid );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error starting process\n" + jvmProcessArgs.conciseToString(), e );
        }
    }

    private static RuntimeException failedToStartExceptionFor( JvmProcessArgs jvmProcessArgs, ProcessWrapper process, JpsPid jpsPid )
    {
        return new RuntimeException( "Failed to start process, and was not reported by 'jps'\n" +
                                     "Process '-Dname' : '" + jvmProcessArgs.processName() + "'\n" +
                                     "------------------  JPS Output  ----------------\n" +
                                     jpsPid.jpsOutput() + "\n" +
                                     "------------------  Process Output  ------------\n" +
                                     process.infoString() );
    }

    private final ProcessWrapper process;
    private final long pid;

    private JvmProcess( ProcessWrapper process, long pid )
    {
        this.process = process;
        this.pid = pid;
    }

    @Override
    public long pid()
    {
        return pid;
    }

    @Override
    public void waitFor()
    {
        process.waitFor();
    }

    @Override
    public void stop()
    {
        process.stop();
    }

    private static JpsPid processPid( Jvm jvm, Instant start, Duration timeout, String processName )
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
                Thread.sleep( 1000 );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving PID for process: " + processName, e );
        }
    }

    private static boolean hasTimedOut( Instant start, Duration warmupTimeout )
    {
        Duration durationWaited = between( start, now() );
        return durationWaited.compareTo( warmupTimeout ) >= 0;
    }

    private static JpsPid processPid( Jvm jvm, String processName ) throws IOException, InterruptedException
    {
        String[] command = {jvm.launchJps(), "-v"};
        Process jpsProcess = new ProcessBuilder( command ).start();
        int resultCode = jpsProcess.waitFor();
        String jpsOutput = inputStreamToString( jpsProcess.getInputStream() );
        if ( resultCode != 0 )
        {
            throw new RuntimeException( "Bad things happened when invoking 'jps'\n" +
                                        "Code   : " + resultCode + "\n" +
                                        "Output : " + jpsOutput );
        }

        return JpsPid.fromProcessOutput( jpsOutput, processName );
    }

    private static class JpsPid
    {
        private static JpsPid fromProcessOutput( String jpsOutput, String processName )
        {
            long pid = jpsOutput.contains( processName )
                       ? pidFromJpsOutput( jpsOutput, processName )
                       : -1;
            return new JpsPid( pid, jpsOutput );
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

        private Optional<Long> pid()
        {
            return (-1 == pid) ? Optional.empty() : Optional.of( pid );
        }

        private String jpsOutput()
        {
            return jpsOutput;
        }
    }

    private static long pidFromJpsOutput( String jpsOutput, String processName )
    {
        String sanitizedJpsOutput = lessWhiteSpace( jpsOutput );
        int indexOfProcessName = sanitizedJpsOutput.indexOf( "-Dname=" + processName );
        // remove anything that appears after the process name we want
        String[] jpsOutputTokens = sanitizedJpsOutput.substring( 0, indexOfProcessName ).split( " " );
        // take the last number token, the one that appears just before our process name
        List<Long> pids = Arrays.stream( jpsOutputTokens )
                                .filter( JvmProcess::isPid )
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
