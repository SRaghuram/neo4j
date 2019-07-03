/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.client.model.Parameters;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.common.Store;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.workload.Query;

import java.nio.file.Path;
import java.util.List;

public class NonForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    NonForkingRunnable( LAUNCHER launcher,
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
        boolean isClientForked = false;
        List<String> commandArgs = launcher.toolArgs( query,
                                                      connection,
                                                      forkDirectory,
                                                      profilerTypes,
                                                      isClientForked,
                                                      neo4jConfigFile,
                                                      resources );

        com.neo4j.bench.macro.Main.main( commandArgs.toArray( new String[0] ) );

        return Results.loadFrom( forkDirectory, Results.Phase.MEASUREMENT );
    }
}
