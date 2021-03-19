/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.macro.execution.measurement.Results;

import java.util.List;

public class NonForkingRunnable<LAUNCHER extends DatabaseLauncher<CONNECTION>, CONNECTION extends AutoCloseable> extends RunnableFork<LAUNCHER,CONNECTION>
{
    NonForkingRunnable( LAUNCHER launcher, ForkDirectory forkDirectory )
    {
        super( launcher, forkDirectory );
    }

    @Override
    protected Results runFork( LAUNCHER launcher,
                               CONNECTION connection,
                               ForkDirectory forkDirectory )
    {
        List<String> commandArgs = launcher.toolArgs( connection, false /* isClientForked */ );

        com.neo4j.bench.macro.Main.main( commandArgs.toArray( new String[0] ) );

        return Results.loadFrom( forkDirectory, RunPhase.MEASUREMENT );
    }
}
