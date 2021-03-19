/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.process.JpsPid;
import com.neo4j.bench.common.process.JvmProcess;
import com.neo4j.bench.common.process.JvmProcessArgs;
import com.neo4j.bench.common.process.PgrepAndPsPid;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.model.process.JvmArgs;

import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;
import java.util.List;

public class ForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    private final Jvm jvm;

    ForkingRunnable( LAUNCHER launcher, ForkDirectory forkDirectory, Jvm jvm )
    {
        super( launcher, forkDirectory );
        this.jvm = jvm;
    }

    @Override
    protected Results runFork( LAUNCHER launcher,
                               CONNECTION connection,
                               ForkDirectory forkDirectory )
    {
        List<String> commandArgs = launcher.toolArgs( connection, true /* isClientForked */ );
        List<String> clientInvokeArgs = launcher.clientInvokeArgs();

        JvmArgs enrichedClientJvmArgs = launcher.clientJvmArgs();

        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( clientInvokeArgs,
                                                                          jvm,
                                                                          enrichedClientJvmArgs,
                                                                          commandArgs,
                                                                          Main.class );

        launcher.beforeClient();

        // inherit output
        Redirect outputRedirect = Redirect.INHERIT;
        // redirect error to file
        Redirect errorRedirect = Redirect.to( forkDirectory.newErrorLog().toFile() );
        JvmProcess jvmProcess = JvmProcess.start(
                jvmProcessArgs,
                outputRedirect,
                errorRedirect,
                Arrays.asList( new JpsPid(), new PgrepAndPsPid() ) );

        // if any, schedule runs of scheduled profilers
        launcher.scheduleProfilers( jvmProcess.pid() );

        try
        {
            jvmProcess.waitFor();
        }
        catch ( Exception e )
        {
            // make sure we clean up in profilers
            launcher.clientFailed();
            // re throw exception
            throw e;
        }

        launcher.afterClient();

        return Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
    }
}
