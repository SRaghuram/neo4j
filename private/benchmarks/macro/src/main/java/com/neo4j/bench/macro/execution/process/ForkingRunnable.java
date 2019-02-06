/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.process.JvmArgs;
import com.neo4j.bench.client.process.JvmProcess;
import com.neo4j.bench.client.process.JvmProcessArgs;
import com.neo4j.bench.client.profiling.ExternalProfiler;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.Main;
import com.neo4j.bench.macro.cli.RunSingleCommand;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static com.neo4j.bench.client.util.Args.concatArgs;
import static java.util.stream.Collectors.toList;

public class ForkingRunnable implements RunnableFork
{
    private final Query query;
    private final ForkDirectory forkDirectory;
    private final List<ProfilerType> profilerTypes;
    private final Store originalStore;
    private final Edition edition;
    private final Path neo4jConfigFile;
    private final Jvm jvm;
    private final int warmupCount;
    private final int measurementCount;
    private final List<String> jvmArgs;
    private final boolean doExportPlan;

    ForkingRunnable( Query query,
                     ForkDirectory forkDirectory,
                     List<ProfilerType> profilerTypes,
                     Store originalStore,
                     Edition edition,
                     Path neo4jConfigFile,
                     Jvm jvm,
                     int warmupCount,
                     int measurementCount,
                     List<String> jvmArgs,
                     boolean doExportPlan )
    {
        this.query = query;
        this.forkDirectory = forkDirectory;
        this.profilerTypes = profilerTypes;
        this.originalStore = originalStore;
        this.edition = edition;
        this.neo4jConfigFile = neo4jConfigFile;
        this.jvm = jvm;
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.jvmArgs = jvmArgs;
        this.doExportPlan = doExportPlan;
    }

    @Override
    public Results run()
    {
        try ( Store store = (query.isMutating() && !query.queryString().executionMode().equals( ExecutionMode.PLAN ))
                            ? originalStore.makeTemporaryCopy()
                            : originalStore )
        {
            List<ProfilerType> internalProfilerTypes = profilerTypes.stream().filter( ProfilerType::isInternal ).collect( toList() );
            List<ProfilerType> externalProfilerTypes = profilerTypes.stream().filter( ProfilerType::isExternal ).collect( toList() );
            List<ExternalProfiler> externalProfilers = externalProfilerTypes.stream()
                                                                            .map( ProfilerType::create )
                                                                            .map( profiler -> (ExternalProfiler) profiler )
                                                                            .collect( toList() );

            List<String> jvmInvokeArgs = externalProfilers.stream()
                                                          .map( profiler -> profiler.jvmInvokeArgs( forkDirectory,
                                                                                                    query.benchmarkGroup(),
                                                                                                    query.benchmark() ) )
                                                          .flatMap( Collection::stream )
                                                          .distinct()
                                                          .collect( toList() );

            List<String> combinedJvmArgs = concatArgs( JvmArgs.standardArgs( forkDirectory ), jvmArgs );
            externalProfilers.stream()
                             .map( profiler -> profiler.jvmArgs( jvm.version(),
                                                                 forkDirectory,
                                                                 query.benchmarkGroup(),
                                                                 query.benchmark() ) )
                             .flatMap( Collection::stream )
                             .filter( profilerJvmArg -> !combinedJvmArgs.contains( profilerJvmArg ) )
                             .forEach( combinedJvmArgs::add );

            List<String> toolCommandArgs = RunSingleCommand.argsFor( query,
                                                                     store,
                                                                     edition,
                                                                     neo4jConfigFile,
                                                                     forkDirectory,
                                                                     internalProfilerTypes,
                                                                     warmupCount,
                                                                     measurementCount,
                                                                     doExportPlan,
                                                                     jvm );

            JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( jvmInvokeArgs,
                                                                              jvm,
                                                                              combinedJvmArgs,
                                                                              toolCommandArgs,
                                                                              Main.class );

            List<ExternalProfiler> profilers = profilerTypes.stream()
                                                            .filter( ProfilerType::isExternal )
                                                            .map( ProfilerType::create )
                                                            .map( profiler -> (ExternalProfiler) profiler )
                                                            .collect( toList() );

            profilers.forEach( profiler -> profiler.beforeProcess( forkDirectory,
                                                                   query.benchmarkGroup(),
                                                                   query.benchmark() ) );

            JvmProcess.start( jvm, jvmProcessArgs ).waitFor();

            profilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                  query.benchmarkGroup(),
                                                                  query.benchmark() ) );

            return Results.loadFrom( forkDirectory, Results.Phase.MEASUREMENT );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error executing benchmark process", e );
        }
    }
}
