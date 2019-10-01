/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;

public class Main
{
    public static void main( String[] args ) throws Exception
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "client" )
                .withDefaultCommand( Help.class )
                .withCommand( ReIndexStoreCommand.class )
                .withCommand( ReportCommand.class )
                .withCommand( AddProfilesCommand.class )
                .withCommand( CompareVersionsCommand.class )
                .withCommand( Help.class );

        builder.withGroup( "annotate" )
               .withDescription( "Adds annotations to :Metrics and/or :TestRun nodes in the results store" )
               .withDefaultCommand( AnnotatePackagingBuildCommand.class )
               .withCommands(
                       AnnotatePackagingBuildCommand.class );

        builder
                .build()
                .parse( args )
                .run();
    }
}
