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
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.assist.ExternalProfilerAssist;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    private final ExternalProfilerAssist clientAssist;

    ForkingRunnable( LAUNCHER launcher,
                     Query query,
                     ForkDirectory forkDirectory,
                     List<ParameterizedProfiler> profilers,
                     Jvm jvm,
                     JvmArgs jvmArgs,
                     Resources resources )
    {
        super( launcher,
               query,
               forkDirectory,
               profilers,
               jvm,
               jvmArgs,
               resources );
        // only provide additional parameters if multiple processes are involved in the benchmark
        Parameters clientParameters = launcher.isDatabaseInDifferentProcess() ? Parameters.CLIENT : Parameters.NONE;
        this.clientAssist = ExternalProfilerAssist.create( createExternalProfilers( profilers ),
                                                           forkDirectory,
                                                           query.benchmarkGroup(),
                                                           query.benchmark(),
                                                           Collections.emptySet(),
                                                           jvm,
                                                           clientParameters );
    }

    @Override
    protected Results runFork( LAUNCHER launcher,
                               CONNECTION connection,
                               Query query,
                               ForkDirectory forkDirectory,
                               List<ParameterizedProfiler> profilers,
                               Jvm jvm,
                               JvmArgs jvmArgs,
                               Resources resources )
    {
        ParameterizedProfiler.assertInternal( profilers );
        boolean isClientForked = true;
        List<String> commandArgs = launcher.toolArgs( query,
                                                      connection,
                                                      ParameterizedProfiler.profilerTypes( profilers ),
                                                      isClientForked,
                                                      resources );
        List<String> clientInvokeArgs = clientAssist.invokeArgs();

        JvmArgs profilersJvmArgs = clientAssist.jvmArgs();
        JvmArgs clientJvmArgs = jvmArgs.merge( profilersJvmArgs );

        JvmArgs enrichedClientJvmArgs = launcher.toolJvmArgs( clientJvmArgs );

        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( clientInvokeArgs,
                                                                          jvm,
                                                                          enrichedClientJvmArgs,
                                                                          commandArgs,
                                                                          Main.class );

        clientAssist.beforeProcess();

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
        clientAssist.schedule( jvmProcess.pid() );

        try
        {
            jvmProcess.waitFor();
        }
        catch ( Exception e )
        {
            // make sure we clean up in profilers
            clientAssist.processFailed();
            // re throw exception
            throw e;
        }

        clientAssist.afterProcess();

        return Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
    }
}
