/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class RestoreDatabaseCliTest
{
    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new RestoreDatabaseCli( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                "USAGE%n" +
                "%n" +
                "restore [--force] [--move] [--verbose] [--database=<database>] --from=<path>%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Restore a backed up database.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose       Enable verbose output.%n" +
                "      --from=<path>   Path to backup to restore from.%n" +
                "      --database=<database>%n" +
                "                      Name of the database to restore.%n" +
                "                        Default: neo4j%n" +
                "      --force         If an existing database should be replaced.%n" +
                "      --move          Moves the backup files to the destination, rather than%n" +
                "                        copying."
        ) ) );
    }
}
