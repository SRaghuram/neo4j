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
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.Profiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.ScheduledProfiler;
import com.neo4j.bench.common.profiling.ScheduledProfilerRunner;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor.create;
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
                                                                                                profilerRecordingDescriptor( query,
                                                                                                                             clientParameters,
                                                                                                                             profiler ) ) )
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
                                                                       profilerRecordingDescriptor( query,
                                                                                                    clientParameters,
                                                                                                    profiler ) ) );

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
        List<ScheduledProfiler> scheduledProfilers = ScheduledProfilerRunner.toScheduleProfilers( externalProfilers );
        ScheduledProfilerRunner scheduledProfilerRunner = new ScheduledProfilerRunner();
        scheduledProfilers.forEach( profiler -> scheduledProfilerRunner.submit( profiler,
                                                                                forkDirectory,
                                                                                profilerRecordingDescriptor( query,
                                                                                                             clientParameters,
                                                                                                             profiler ),
                                                                                jvm,
                                                                                jvmProcess.pid() ) );

        try
        {
            jvmProcess.waitFor();
        }
        catch ( Exception e )
        {
            // make sure we clean up in profilers
            externalProfilers.forEach( profiler -> profiler.processFailed( forkDirectory,
                                                                           profilerRecordingDescriptor( query,
                                                                                                        clientParameters,
                                                                                                        profiler ) ) );
            // re throw exception
            throw e;
        }
        finally
        {
            // stop scheduled profilers
            scheduledProfilerRunner.stop();
        }

        externalProfilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                      profilerRecordingDescriptor( query, clientParameters, profiler ) ) );

        return Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
    }

    private ProfilerRecordingDescriptor profilerRecordingDescriptor( Query query, Parameters parameters, Profiler profiler )
    {
        return create( query.benchmarkGroup(),
                       query.benchmark(),
                       RunPhase.MEASUREMENT,
                       ParameterizedProfiler.defaultProfiler( ProfilerType.typeOf( profiler ) ),
                       parameters );
    }
}
