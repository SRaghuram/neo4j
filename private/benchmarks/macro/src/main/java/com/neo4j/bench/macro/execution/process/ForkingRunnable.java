/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Parameters;
import com.neo4j.bench.client.process.JvmProcess;
import com.neo4j.bench.client.process.JvmProcessArgs;
import com.neo4j.bench.client.profiling.ExternalProfiler;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;
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

        System.out.println( format( "Original JVM args are %s", jvmArgs ) );

        List<String> clientInvokeArgs = externalProfilers.stream()
                                                         .map( profiler -> profiler.invokeArgs( forkDirectory,
                                                                                                query.benchmarkGroup(),
                                                                                                query.benchmark(),
                                                                                                clientParameters ) )
                                                         .flatMap( Collection::stream )
                                                         .distinct()
                                                         .collect( toList() );

        System.out.println( format( "Client JVM invoke args are %s", clientInvokeArgs ) );

        List<String> clientJvmArgs = RunnableFork.addExternalProfilerJvmArgs( externalProfilers,
                                                                              jvm,
                                                                              forkDirectory,
                                                                              query,
                                                                              clientParameters,
                                                                              jvmArgs );

        System.out.println( format( "Client JVM args are %s", clientJvmArgs ) );

        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( clientInvokeArgs,
                                                                          jvm,
                                                                          clientJvmArgs,
                                                                          commandArgs,
                                                                          Main.class );

        System.out.println( format( "Client JVM args are %s", jvmProcessArgs ) );

        externalProfilers.forEach( profiler -> profiler.beforeProcess( forkDirectory,
                                                                       query.benchmarkGroup(),
                                                                       query.benchmark(),
                                                                       clientParameters ) );

        // inherit output
        Redirect outputRedirect = Redirect.INHERIT;
        // redirect error to file
        Redirect errorRedirect = Redirect.to( forkDirectory.newErrorLog().toFile() );
        JvmProcess.start( jvmProcessArgs,
                          outputRedirect,
                          errorRedirect ).waitFor();

        externalProfilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                      query.benchmarkGroup(),
                                                                      query.benchmark(),
                                                                      clientParameters ) );

        return Results.loadFrom( forkDirectory, Results.Phase.MEASUREMENT );
    }
}
