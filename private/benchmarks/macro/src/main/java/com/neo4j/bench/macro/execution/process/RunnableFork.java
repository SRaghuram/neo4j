/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.OOMProfiler;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.List;

import static com.neo4j.bench.common.profiling.ParameterizedProfiler.defaultProfiler;
import static com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor.create;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public abstract class RunnableFork<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable>
{
    private final Query query;
    private final ForkDirectory forkDirectory;
    private final List<ParameterizedProfiler> profilers;
    private final List<ExternalProfiler> externalProfilers;
    private final Jvm jvm;
    private final JvmArgs jvmArgs;
    private final LAUNCHER launcher;
    private final Resources resources;

    RunnableFork( LAUNCHER launcher,
                  Query query,
                  ForkDirectory forkDirectory,
                  List<ParameterizedProfiler> profilers,
                  Jvm jvm,
                  JvmArgs jvmArgs,
                  Resources resources )
    {
        this.query = query;
        this.forkDirectory = forkDirectory;
        this.profilers = profilers;
        this.externalProfilers = ProfilerType.createExternalProfilers( profilers.stream().map( ParameterizedProfiler::profilerType ).collect( toList() ) );
        this.jvm = jvm;
        this.jvmArgs = JvmArgs.standardArgs()
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
                       profilers.isEmpty() ? "-" : ParameterizedProfiler.serialize( profilers ) );
    }

    public final Results run()
    {
        try
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
            // TODO filter out OOMProfiler for server, as we don't support spaces in additional jmv args
            // https://trello.com/c/NF6by0ki/5084-dbmsjvmadditional-with-spaces-in-them
            List<ExternalProfiler> filteredExternalProfilers = externalProfilers.stream()
                                                                                .filter( profiler -> profiler.getClass() != OOMProfiler.class )
                                                                                .collect( toList() );
            List<String> serverJvmArgs = RunnableFork.addExternalProfilerJvmArgs(
                    filteredExternalProfilers,
                    jvm,
                    forkDirectory,
                    query,
                    serverParameters,
                    jvmArgs,
                    resources )
                                                     .toArgs();

            if ( launcher.isDatabaseInDifferentProcess() )
            {
                // There are two processes to profile
                // Both recordings may be the same, if it is a system-wide profiler
                // But it is important that each recording is named accordingly, so profilers must be started with the correct parameters
                externalProfilers.forEach( profiler -> profiler.beforeProcess( forkDirectory,
                                                                               create( query.benchmarkGroup(),
                                                                                       query.benchmark(),
                                                                                       RunPhase.MEASUREMENT,
                                                                                       defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                                                       serverParameters ) ) );
            }

            try ( CONNECTION connection = launcher.initDatabaseServer( serverJvmArgs ) )
            {
                return runFork( launcher,
                                connection,
                                query,
                                forkDirectory,
                                ParameterizedProfiler.internalProfilers( profilers ),
                                jvm,
                                jvmArgs,
                                clientParameters,
                                resources );
            }
            finally
            {
                if ( launcher.isDatabaseInDifferentProcess() )
                {
                    externalProfilers.forEach( profiler -> profiler.afterProcess( forkDirectory,
                                                                                  create( query.benchmarkGroup(),
                                                                                          query.benchmark(),
                                                                                          RunPhase.MEASUREMENT,
                                                                                          defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                                                          serverParameters ) ) );
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
                                        List<ParameterizedProfiler> profilers,
                                        Jvm jvm,
                                        JvmArgs jvmArgs,
                                        Parameters clientParameters,
                                        Resources resources );

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
                                                                                        create( query.benchmarkGroup(),
                                                                                                query.benchmark(),
                                                                                                RunPhase.MEASUREMENT,
                                                                                                defaultProfiler( ProfilerType.typeOf( profiler ) ),
                                                                                                parameters ),
                                                                                        resources ) )
                                                    .reduce( JvmArgs.empty(), JvmArgs::merge );

        return jvmArgs.merge( profilersJvmArgs );
    }
}
