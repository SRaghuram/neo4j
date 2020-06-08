/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.process.JpsPid;
import com.neo4j.bench.common.process.JvmProcess;
import com.neo4j.bench.common.process.JvmProcessArgs;
import com.neo4j.bench.common.process.PgrepAndPsPid;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ScheduledProfilerRunner;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    ForkingRunnable( LAUNCHER launcher,
                     Query query,
                     ForkDirectory forkDirectory,
                     List<ParameterizedProfiler> profilers,
                     Store originalStore,
                     Path neo4jConfigFile,
                     Jvm jvm,
                     JvmArgs jvmArgs,
                     Resources resources )
    {
        super( launcher,
               query,
               forkDirectory,
               profilers,
               originalStore,
               neo4jConfigFile,
               jvm,
               jvmArgs,
               resources );
    }

    @Override
    protected Results runFork( LAUNCHER launcher,
                               CONNECTION connection,
                               Query query,
                               ForkDirectory forkDirectory,
                               List<ParameterizedProfiler> profilers,
                               Jvm jvm,
                               Path neo4jConfigFile,
                               JvmArgs jvmArgs,
                               Parameters clientParameters,
                               Resources resources )
    {
        ParameterizedProfiler.assertInternal( profilers );
        boolean isClientForked = true;
        List<String> commandArgs = launcher.toolArgs( query,
                                                      connection,
                                                      forkDirectory,
                                                      ParameterizedProfiler.profilerTypes( profilers ),
                                                      isClientForked,
                                                      neo4jConfigFile,
                                                      resources );
        // retrieves same external profiler _instances_ that were already used -- because external profilers are allowed to be stateful
        List<ExternalProfiler> externalProfilers = externalProfilers();
        List<String> clientInvokeArgs = externalProfilers.stream()
                                                         .map( profiler -> profiler.invokeArgs( forkDirectory,
                                                                                                query.benchmarkGroup(),
                                                                                                query.benchmark(),
                                                                                                clientParameters ) )
                                                         .flatMap( Collection::stream )
                                                         .distinct()
                                                         .collect( toList() );

        JvmArgs clientJvmArgs = RunnableFork.addExternalProfilerJvmArgs( externalProfilers,
                                                                         jvm,
                                                                         forkDirectory,
                                                                         query,
                                                                         clientParameters,
                                                                         jvmArgs,
                                                                         resources );

        JvmArgs enrichedClientJvmArgs = launcher.toolJvmArgs( clientJvmArgs );

        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( clientInvokeArgs,
                                                                          jvm,
                                                                          enrichedClientJvmArgs,
                                                                          commandArgs,
                                                                          Main.class );

        externalProfilers.forEach( profiler -> profiler.beforeProcess( forkDirectory,
                                                                       query.benchmarkGroup(),
                                                                       query.benchmark(),
                                                                       clientParameters ) );

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
        ScheduledProfilerRunner scheduledProfilersRunner = ScheduledProfilerRunner.from( externalProfilers );
        scheduledProfilersRunner.start( forkDirectory, query.benchmarkGroup(), query.benchmark(), clientParameters, jvm, jvmProcess.pid() );

        try
        {
            jvmProcess.waitFor();
        }
        catch ( Exception e )
        {
            // make sure we clean up in profilers
            externalProfilers.forEach( profiler -> profiler.processFailed( forkDirectory,
                                                                           query.benchmarkGroup(),
                                                                           query.benchmark(),
                                                                           clientParameters ) );
            // re throw exception
            throw e;
        }
        finally
        {
            // stop scheduled profilers
            scheduledProfilersRunner.stop();
        }

        externalProfilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                      query.benchmarkGroup(),
                                                                      query.benchmark(),
                                                                      clientParameters ) );

        return Results.loadFrom( forkDirectory, Results.Phase.MEASUREMENT );
    }
}
