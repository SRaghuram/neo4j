/*
 * Copyright (c) "Neo4j"
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
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }

        assertThat( baos.toString().trim(), equalTo( String.format(
                "USAGE%n" +
                "%n" +
                "restore [--force] [--move] [--verbose] [--database=<database>]%n" +
                "        [--to-data-directory=<path>] [--to-data-tx-directory=<path>]%n" +
                "        --from=<path>[,<path>...]...%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Restore a backed up database.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose   Enable verbose output.%n" +
                "      --from=<path>[,<path>...]...%n" +
                "                  Path or paths from which to restore. Every path can contain%n" +
                "                    asterisks or question marks in the last subpath. Multiple%n" +
                "                    paths may be separated by a comma, but paths themselves%n" +
                "                    must not contain commas.%n" +
                "      --database=<database>%n" +
                "                  Name of the database after restore. Usage of this option is%n" +
                "                    only allowed if --from parameter point to exact one%n" +
                "                    directory%n" +
                "      --force     If an existing database should be replaced.%n" +
                "      --move      Moves the backup files to the destination, rather than%n" +
                "                    copying.%n" +
                "      --to-data-directory=<path>%n" +
                "                  Base directory for databases. Usage of this option is only%n" +
                "                    allowed if --from parameter point to exact one directory%n" +
                "      --to-data-tx-directory=<path>%n" +
                "                  Base directory for transaction logs. Usage of this option is%n" +
                "                    only allowed if --from parameter point to exact one%n" +
                "                    directory"
        ) ) );
    }
}
