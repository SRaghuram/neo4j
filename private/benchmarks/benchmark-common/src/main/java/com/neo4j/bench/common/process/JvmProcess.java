/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import java.lang.ProcessBuilder.Redirect;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

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
            JpsPid jpsPid = new JpsPid();
            jpsPid.tryFindFor( jvmProcessArgs.jvm(), start, timeout, jvmProcessArgs.processName() );
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
        // It may be that the process started, but could not be discovered via JPS
        // Make every effort to ensure it is not running
        process.stop();
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
