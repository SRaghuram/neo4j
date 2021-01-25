/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.worker;

import com.github.rvesse.airline.Cli;

public class Main
{
    public static void main( String[] args )
    {

        Cli<Runnable> cli = Cli.<Runnable>builder( "run-bench" )
                .withCommand( RunWorkerCommand.class )
                .build();
        Runnable command = cli.parse( args );
        command.run();
    }
}
