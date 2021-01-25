/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.google.common.collect.Lists;

import java.lang.ProcessBuilder.Redirect;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class JvmProcess implements BaseProcess, HasPid
{
    public static JvmProcess start( JvmProcessArgs jvmProcessArgs )
    {
        return start( jvmProcessArgs, Redirect.INHERIT, Redirect.INHERIT, Lists.asList( new JpsPid(), new PgrepAndPsPid[]{new PgrepAndPsPid()} ) );
    }

    public static JvmProcess start( JvmProcessArgs jvmProcessArgs, Redirect outputRedirect, Redirect errorRedirect, List<PidStrategy> discoverPidStrategy )
    {
        if ( discoverPidStrategy.isEmpty() )
        {
            throw new RuntimeException( "Error starting process. Expected at least one discoverPidStrategy but got none" );
        }

        try
        {
            ProcessBuilder processBuilder = new ProcessBuilder()
                    .command( jvmProcessArgs.args() )
                    .redirectOutput( outputRedirect )
                    .redirectError( errorRedirect );
            ProcessWrapper process = ProcessWrapper.start( processBuilder );
            long pid = getPid( jvmProcessArgs, process, discoverPidStrategy );
            return new JvmProcess( process, pid );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error starting process\n" + jvmProcessArgs.conciseToString(), e );
        }
    }

    private static Long getPid( JvmProcessArgs jvmProcessArgs, ProcessWrapper process, List<PidStrategy> discoverPidStrategy )
    {
        Duration timeout = Duration.of( 5, ChronoUnit.MINUTES );
        boolean haveFoundPid = false;
        Iterator<PidStrategy> pidIterator = discoverPidStrategy.iterator();
        PidStrategy pidStrategy = null;
        while ( !haveFoundPid && pidIterator.hasNext() )
        {
            pidStrategy = pidIterator.next();
            Instant start = Instant.now();
            pidStrategy.tryFindFor( jvmProcessArgs.jvm(), start, timeout, jvmProcessArgs.processName() );
            haveFoundPid = pidStrategy.pid().isPresent();
        }
        if ( pidStrategy.pid().isPresent() )
        {
            return pidStrategy.pid().get();
        }
        else
        {
            process.stop();
            throw new RuntimeException( "Failed to start process, and was not reported by 'jps'\n" +
                                        "Process '-Dname' : '" + jvmProcessArgs.processName() + "'\n" +
                                        "------------------  JPS Output  ----------------\n" +
                                        discoverPidStrategy.stream().
                                                map( PidStrategy::toString ).collect( Collectors.joining( "\n ---------------------- \n" ) ) +
                                        "\n" +
                                        "------------------  Process Output  ------------\n" +
                                        process.infoString() );
        }
    }

    private final ProcessWrapper process;
    private final long pid;

    private JvmProcess( ProcessWrapper process, long pid )
    {
        this.process = process;
        this.pid = pid;
    }

    @Override
    public Pid pid()
    {
        return new Pid( pid );
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
}
