/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

public class Main
{
    public static void main( String[] args ) throws Exception
    {
        Cli.<Runnable>builder( "client" )
                .withDefaultCommand( Help.class )
                .withCommand( ReIndexStoreCommand.class )
                .withCommand( ReportCommand.class )
                .withCommand( AddProfilesCommand.class )
                .withCommand( Help.class )
                .build()
                .parse( args )
                .run();
    }
}
