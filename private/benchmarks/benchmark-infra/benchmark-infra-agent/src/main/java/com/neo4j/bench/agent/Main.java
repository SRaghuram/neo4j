/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.neo4j.bench.agent.cli.RunServerAgentCommand;

public class Main
{
    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "agent" )
                .withDefaultCommand( Help.class )
                .withCommands(
                        RunServerAgentCommand.class,
                        Help.class );

        builder.build()
               .parse( args )
               .run();
    }
}
