package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.profiling.ExternalProfiler;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.cli.RunSingleCommand;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class NonForkingRunnable implements RunnableFork
{
    private final Query query;
    private final Store originalStore;
    private final Edition edition;
    private final Path neo4jConfigFile;
    private final ForkDirectory forkDirectory;
    private final List<ProfilerType> profilerTypes;
    private final int warmupCount;
    private final int measurementCount;
    private final boolean doExportPlan;
    private final Jvm jvm;

    NonForkingRunnable( Query query,
                        ForkDirectory forkDirectory,
                        List<ProfilerType> profilerTypes,
                        Store originalStore,
                        Edition edition,
                        Path neo4jConfigFile,
                        int warmupCount,
                        int measurementCount,
                        boolean doExportPlan,
                        Jvm jvm )
    {
        this.query = query;
        this.originalStore = originalStore;
        this.edition = edition;
        this.neo4jConfigFile = neo4jConfigFile;
        this.forkDirectory = forkDirectory;
        this.profilerTypes = profilerTypes;
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.doExportPlan = doExportPlan;
        this.jvm = jvm;
    }

    @Override
    public Results run()
    {
        try ( Store store = (query.isMutating() && !query.queryString().executionMode().equals( ExecutionMode.PLAN ))
                            ? originalStore.makeTemporaryCopy()
                            : originalStore )
        {
            List<ProfilerType> internalProfilerTypes = profilerTypes.stream().filter( ProfilerType::isInternal ).collect( toList() );
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

            List<ExternalProfiler> profilers = profilerTypes.stream()
                                                            .filter( ProfilerType::isExternal )
                                                            .map( ProfilerType::create )
                                                            .map( profiler -> (ExternalProfiler) profiler )
                                                            .collect( toList() );

            if ( !profilers.isEmpty() )
            {
                List<String> profilerNames = profilerTypes.stream()
                                                          .filter( ProfilerType::isExternal )
                                                          .map( ProfilerType::name )
                                                          .collect( toList() );
                String warningMessage = "-------------------------------------------------------------------------------------------------\n" +
                                        "-------------------------------------------  WARNING  -------------------------------------------\n" +
                                        "-------------------------------------------------------------------------------------------------\n" +
                                        "Profilers that depend on 'JVM Args' and/or 'JVM Invoke Args' may not work in non-forking mode\n" +
                                        "Currently running with profilers: " + profilerNames + "\n" +
                                        "-------------------------------------------------------------------------------------------------";
                System.out.println( warningMessage );
            }

            profilers.forEach( profiler -> profiler.beforeProcess( forkDirectory,
                                                                   query.benchmarkGroup(),
                                                                   query.benchmark() ) );

            com.neo4j.bench.macro.Main.main( toolCommandArgs.toArray( new String[0] ) );

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
