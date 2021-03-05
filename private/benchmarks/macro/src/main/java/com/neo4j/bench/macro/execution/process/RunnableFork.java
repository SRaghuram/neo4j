/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.assist.ExternalProfilerAssist;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public abstract class RunnableFork<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable>
{
    private final Query query;
    private final ForkDirectory forkDirectory;
    private final List<ParameterizedProfiler> profilers;
    private final Jvm jvm;
    private final JvmArgs jvmArgs;
    private final LAUNCHER launcher;
    private final Resources resources;
    private final ExternalProfilerAssist serverAssist;

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
        this.jvm = jvm;
        this.jvmArgs = JvmArgs.standardArgs()
                              .merge( jvmArgs )
                              // every fork will have its own temporary directory
                              .set( format( "-Djava.io.tmpdir=%s", BenchmarkUtil.tryMkDir( forkDirectory.pathFor( "tmp" ) ) ) );
        this.launcher = launcher;
        this.resources = resources;
        this.serverAssist = ExternalProfilerAssist.create( createExternalProfilers( profilers ),
                                                           forkDirectory,
                                                           query.benchmarkGroup(),
                                                           query.benchmark(),
                                                           Collections.emptySet(),
                                                           jvm,
                                                           Parameters.SERVER );
    }

    protected static List<ExternalProfiler> createExternalProfilers( List<ParameterizedProfiler> profilers )
    {
        return ProfilerType.createExternalProfilers( profilers.stream()
                                                              .map( ParameterizedProfiler::profilerType )
                                                              .collect( Collectors.toList() ) );
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

            // TODO currently process invoke arguments from external profilers are only applied to the client, not the server
//            List<String> serverInvokeArgs = externalProfilers.stream()
//                                                             .map( profiler -> profiler.invokeArgs( forkDirectory,
//                                                                                                    query.benchmarkGroup(),
//                                                                                                    query.benchmark(),
//                                                                                                    serverParameters ) )
//                                                             .flatMap( Collection::stream )
//                                                             .distinct()
//                                                             .collect( toList() );
            JvmArgs profilersJvmArgs = serverAssist.jvmArgsWithoutOOM();
            List<String> serverJvmArgs = jvmArgs.merge( profilersJvmArgs ).toArgs();

            if ( launcher.isDatabaseInDifferentProcess() )
            {
                // There are two processes to profile
                // Both recordings may be the same, if it is a system-wide profiler
                // But it is important that each recording is named accordingly, so profilers must be started with the correct parameters
                serverAssist.beforeProcess();
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
                                resources );
            }
            finally
            {
                if ( launcher.isDatabaseInDifferentProcess() )
                {
                    serverAssist.afterProcess();
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to launch fork", e );
        }
    }

    protected abstract Results runFork( LAUNCHER launcher,
                                        CONNECTION connection,
                                        Query query,
                                        ForkDirectory forkDirectory,
                                        List<ParameterizedProfiler> profilers,
                                        Jvm jvm,
                                        JvmArgs jvmArgs,
                                        Resources resources );
}
