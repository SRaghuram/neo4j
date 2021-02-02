/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.neo4j.bench.client.cli.annotate.AnnotatePackagingBuildCommand;
import com.neo4j.bench.client.cli.refactor.MoveBenchmarkCommand;
import com.neo4j.bench.client.cli.refactor.VerifySchemaCommand;

import java.util.List;

public class Main
{
    public static void main( List<String> args )
    {
        main( args.toArray( new String[]{} ) );
    }

    public static void main( String[] args )
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder( "client" )
                .withDefaultCommand( Help.class )
                .withCommand( ReIndexStoreCommand.class )
                .withCommand( CompareVersionsCommand.class )
                .withCommand( ReportTestRunCommand.class )
                .withCommand( Help.class );

        builder.withGroup( "annotate" )
               .withDescription( "Adds annotations to :Metrics and/or :TestRun nodes in the results store" )
               .withDefaultCommand( AnnotatePackagingBuildCommand.class )
               .withCommand( AnnotatePackagingBuildCommand.class );

        builder.withGroup( "refactor" )
               .withDescription( "Allows to modify existing store." )
               .withDefaultCommand( VerifySchemaCommand.class )
               .withCommand( MoveBenchmarkCommand.class )
               .withCommand( VerifySchemaCommand.class );

        builder
                .build()
                .parse( args )
                .run();
    }
}
