/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.util.List;

import static java.lang.String.format;

public abstract class RunnableFork<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable>
{
    private final Query query;
    private final ForkDirectory forkDirectory;
    private final List<ProfilerType> profilerTypes;
    private final List<ExternalProfiler> externalProfilers;
    private final Store originalStore;
    private final Path neo4jConfigFile;
    private final Jvm jvm;
    private final JvmArgs jvmArgs;
    private final LAUNCHER launcher;
    private final Resources resources;

    RunnableFork( LAUNCHER launcher,
                  Query query,
                  ForkDirectory forkDirectory,
                  List<ProfilerType> profilerTypes,
                  Store originalStore,
                  Path neo4jConfigFile,
                  Jvm jvm,
                  JvmArgs jvmArgs,
                  Resources resources )
    {
        this.query = query;
        this.forkDirectory = forkDirectory;
        this.profilerTypes = profilerTypes;
        this.externalProfilers = ProfilerType.createExternalProfilers( profilerTypes );
        this.originalStore = originalStore;
        this.neo4jConfigFile = neo4jConfigFile;
        this.jvm = jvm;
        this.jvmArgs = JvmArgs.standardArgs( forkDirectory )
                .merge( jvmArgs )
                // every fork will have its own temporary directory
                .set( format( "-Djava.io.tmpdir=%s", BenchmarkUtil.tryMkDir( forkDirectory.pathFor( "tmp" ) ) ) );
        this.launcher = launcher;
        this.resources = resources;
    }

    @Override
    public String toString()
    {
        return format( "Fork (%s)\n" +
                       "  Directory: %s\n" +
                       "  Query:     %s\n" +
                       "  Profilers: %s",
                       getClass().getSimpleName(),
                       forkDirectory.toAbsolutePath(),
                       query.name(),
                       profilerTypes.isEmpty() ? "-" : ProfilerType.serializeProfilers( profilerTypes ) );
    }

    public final Results run()
    {
        boolean isPlanningMode = query.queryString().executionMode().equals( ExecutionMode.PLAN );
        try ( Store store = (query.isMutating() && !isPlanningMode)
                            ? originalStore.makeTemporaryCopy()
                            : originalStore )
        {
            // only provide additional parameters if multiple processes are involved in the benchmark
            Parameters serverParameters = Parameters.SERVER;
            Parameters clientParameters = launcher.isDatabaseInDifferentProcess() ? Parameters.CLIENT : Parameters.NONE;

            // TODO currently process invoke arguments from external profilers are only applied to the client, not the server
//            List<String> serverInvokeArgs = externalProfilers.stream()
//                                                             .map( profiler -> profiler.invokeArgs( forkDirectory,
//                                                                                                    query.benchmarkGroup(),
//                                                                                                    query.benchmark(),
//                                                                                                    serverParameters ) )
//                                                             .flatMap( Collection::stream )
//                                                             .distinct()
//                                                             .collect( toList() );
            List<String> serverJvmArgs = RunnableFork.addExternalProfilerJvmArgs( externalProfilers,
                                                                                  jvm,
                                                                                  forkDirectory,
                                                                                  query,
                                                                                  serverParameters,
                                                                                  jvmArgs,
                                                                                  resources)
                                                                    .toArgs();

            if ( launcher.isDatabaseInDifferentProcess() )
            {
                // There are two processes to profile
                // Both recordings may be the same, if it is a system-wide profiler
                // But it is important that each recording is named accordingly, so profilers must be started with the correct parameters
                externalProfilers.forEach( profiler -> profiler.beforeProcess( forkDirectory,
                                                                               query.benchmarkGroup(),
                                                                               query.benchmark(),
                                                                               serverParameters ) );
            }

            try ( CONNECTION connection = launcher.initDatabaseServer( jvm, store, neo4jConfigFile, forkDirectory, serverJvmArgs ) )
            {
                return runFork( launcher,
                                connection,
                                query,
                                forkDirectory,
                                ProfilerType.internalProfilers( profilerTypes ),
                                jvm,
                                neo4jConfigFile,
                                jvmArgs,
                                clientParameters,
                                resources );
            }
            finally
            {
                unsanitizeProfilerRecordings( clientParameters );
                if ( launcher.isDatabaseInDifferentProcess() )
                {
                    externalProfilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                                  query.benchmarkGroup(),
                                                                                  query.benchmark(),
                                                                                  serverParameters ) );
                    unsanitizeProfilerRecordings( serverParameters );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to launch fork", e );
        }
    }

    // make external profiler _instances_ available to extending classes -- because external profilers are allowed to be stateful
    List<ExternalProfiler> externalProfilers()
    {
        return externalProfilers;
    }

    protected abstract Results runFork( LAUNCHER launcher,
                                        CONNECTION connection,
                                        Query query,
                                        ForkDirectory forkDirectory,
                                        List<ProfilerType> profilerTypes,
                                        Jvm jvm,
                                        Path neo4jConfigFile,
                                        JvmArgs jvmArgs,
                                        Parameters clientParameters,
                                        Resources resources );

    /**
     * Renames profiler recording names into their parsable forms.
     * This is necessary because profilers generate sanitized filenames, so various tools (e.g., JVM, Async, JFR) do not complain.
     */
    private void unsanitizeProfilerRecordings( Parameters additionalParameters )
    {
        for ( ProfilerType profilerType : profilerTypes )
        {
            forkDirectory.unsanitizeProfilerRecordingsFor( query.benchmarkGroup(),
                                                           query.benchmark(),
                                                           profilerType,
                                                           additionalParameters );
        }
    }

    static JvmArgs addExternalProfilerJvmArgs( List<ExternalProfiler> externalProfilers,
                                                    Jvm jvm,
                                                    ForkDirectory forkDirectory,
                                                    Query query,
                                                    Parameters parameters,
                                                    JvmArgs jvmArgs,
                                                    Resources resources )
    {
        JvmArgs profilersJvmArgs = externalProfilers.stream()
                         .map( profiler -> profiler.jvmArgs( jvm.version(),
                                                             forkDirectory,
                                                             query.benchmarkGroup(),
                                                             query.benchmark(),
                                                             parameters,
                                                             resources ) )
                         .reduce( JvmArgs.empty(), JvmArgs::merge );

        return jvmArgs.merge( profilersJvmArgs );
    }
}
