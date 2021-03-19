/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.macro.execution.measurement.Results;

import static java.lang.String.format;

public abstract class RunnableFork<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable>
{
    private final ForkDirectory forkDirectory;
    private final LAUNCHER launcher;

    RunnableFork( LAUNCHER launcher, ForkDirectory forkDirectory )
    {
        this.forkDirectory = forkDirectory;
        this.launcher = launcher;
    }

    @Override
    public String toString()
    {
        return format( "Fork (%s)\n" +
                       "  Directory: %s",
                       getClass().getSimpleName(),
                       forkDirectory.toAbsolutePath() );
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
            try ( CONNECTION connection = launcher.initDatabaseServer() )
            {
                return runFork( launcher,
                                connection,
                                forkDirectory );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error trying to launch fork", e );
        }
    }

    protected abstract Results runFork( LAUNCHER launcher,
                                        CONNECTION connection,
                                        ForkDirectory forkDirectory );
}
