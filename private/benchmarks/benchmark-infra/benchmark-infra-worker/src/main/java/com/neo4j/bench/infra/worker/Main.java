/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.worker;

import com.github.rvesse.airline.Cli;
import com.neo4j.bench.infra.InfraCommand;

/**
 *
 */
public class Main
{
    public static void main( String[] args )
    {

        Cli<InfraCommand> cli = Cli.<InfraCommand>builder( "run-bench" )
                .withCommand( RunWorker.class )
                .build();
        InfraCommand command = cli.parse( args );
        command.run();

    }

}
