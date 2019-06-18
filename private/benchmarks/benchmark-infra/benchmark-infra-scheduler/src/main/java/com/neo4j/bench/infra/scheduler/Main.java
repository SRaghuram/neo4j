/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.neo4j.bench.infra.InfraCommand;
import io.airlift.airline.Cli;

/**
 *
 */
public class Main
{
    public static void main( String[] args )
    {

        Cli<InfraCommand> cli = Cli.<InfraCommand>builder( "run-bench" )
                .withCommand( ScheduleMacro.class )
                .build();
        InfraCommand command = cli.parse( args );
        command.run();

    }

}
