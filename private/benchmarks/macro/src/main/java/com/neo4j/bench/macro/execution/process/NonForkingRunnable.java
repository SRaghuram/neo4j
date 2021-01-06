/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;

import java.nio.file.Path;
import java.util.List;

public class NonForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    NonForkingRunnable( LAUNCHER launcher,
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
        boolean isClientForked = false;
        List<String> commandArgs = launcher.toolArgs( query,
                                                      connection,
                                                      forkDirectory,
                                                      ParameterizedProfiler.profilerTypes( profilers ),
                                                      isClientForked,
                                                      neo4jConfigFile,
                                                      resources );

        com.neo4j.bench.macro.Main.main( commandArgs.toArray( new String[0] ) );

        return Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
    }
}
