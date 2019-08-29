/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.process.JvmProcess;
import com.neo4j.bench.common.process.JvmProcessArgs;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    ForkingRunnable( LAUNCHER launcher,
                     Query query,
                     ForkDirectory forkDirectory,
                     List<ProfilerType> profilerTypes,
                     Store originalStore,
                     Path neo4jConfigFile,
                     Jvm jvm,
                     List<String> jvmArgs,
                     Resources resources )
    {
        super( launcher,
               query,
               forkDirectory,
               profilerTypes,
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
                               List<ProfilerType> profilerTypes,
                               Jvm jvm,
                               Path neo4jConfigFile,
                               List<String> jvmArgs,
                               Parameters clientParameters,
                               Resources resources )
    {
        ProfilerType.assertInternal( profilerTypes );
        boolean isClientForked = true;
        List<String> commandArgs = launcher.toolArgs( query,
                                                      connection,
                                                      forkDirectory,
                                                      profilerTypes,
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

        List<String> clientJvmArgs = RunnableFork.addExternalProfilerJvmArgs( externalProfilers,
                                                                              jvm,
                                                                              forkDirectory,
                                                                              query,
                                                                              clientParameters,
                                                                              jvmArgs );

        List<String> enrichedClientJvmArgs = JvmArgs.from( clientJvmArgs )
              .set( "-Xmx2g" )
              .set( "-Xms2g" )
              .toArgs();

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
        JvmProcess jvmProcess = JvmProcess.start( jvmProcessArgs,
                          outputRedirect,
                          errorRedirect );

        // if any, schedule runs of scheduled profilers
        ScheduledProfilers schedulerProfilers = ScheduledProfilers.from(externalProfilers);
        schedulerProfilers.start( forkDirectory, query.benchmarkGroup(), query.benchmark(), clientParameters, jvmProcess.pid() );

        jvmProcess.waitFor();

        // stop scheduled profilers
        schedulerProfilers.stop();

        externalProfilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                      query.benchmarkGroup(),
                                                                      query.benchmark(),
                                                                      clientParameters ) );

        return Results.loadFrom( forkDirectory, Results.Phase.MEASUREMENT );
    }
}
