/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.TimeZone;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;

@TestDirectoryExtension
class OnlineBackupCommandTest
{
    @Inject
    private TestDirectory dir;
    @Inject
    private FileSystemAbstraction fs;

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new OnlineBackupCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( format(
                "Perform an online backup from a running Neo4j enterprise server.%n%n" +
                        "USAGE%n" + "%n" +
                        "backup [--check-consistency] [--fallback-to-full] [--verbose]%n" +
                        "       [--additional-config=<path>] --backup-dir=<path>%n" +
                        "       [--check-graph=<true/false>] [--check-indexes=<true/false>]%n" +
                        "       [--check-label-scan-store=<true/false>]%n" +
                        "       [--check-property-owners=<true/false>] [--database=<database>]%n" +
                        "       [--from=<host:port>] [--pagecache=<size>] [--report-dir=<path>]%n%n"
                        + "DESCRIPTION%n" + "%n" +
                        "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup%n" +
                        "service must have been configured on the server beforehand.%n%n" +
                        "All consistency checks except 'cc-graph' can be quite expensive so it may be%n" +
                        "useful to turn them off for very large databases. Increasing the heap size can%n" +
                        "also be a good idea. See 'neo4j-admin help' for details.%n%n"
                        + "For more information see: https://neo4j.%ncom/docs/operations-manual/current/backup/%n" + "%n"
                        + "OPTIONS%n%n" +
                        "      --verbose             Enable verbose output.%n" +
                        "      --backup-dir=<path>   Directory to place backup in.%n" +
                        "      --from=<host:port>    Host and port of Neo4j.%n" +
                        "                              Default: localhost:6362%n" +
                        "      --database=<database> Name of the remote database to backup.%n" +
                        "                              Default: neo4j%n" +
                        "      --fallback-to-full    If an incremental backup fails backup will move the%n" +
                        "                              old backup to <name>.err.<N> and fallback to a%n" +
                        "                              full.%n" +
                        "                              Default: true%n" +
                        "      --pagecache=<size>    The size of the page cache to use for the backup%n" +
                        "                              process.%n" +
                        "                              Default: 8m%n" +
                        "      --check-consistency   If a consistency check should be made.%n" +
                        "                              Default: true%n" +
                        "      --report-dir=<path>   Directory where consistency report will be written.%n" +
                        "                              Default: .%n" +
                        "      --check-graph=<true/false>%n" +
                        "                            Perform consistency checks between nodes,%n" +
                        "                              relationships, properties, types and tokens.%n" +
                        "                              Default: true%n" +
                        "      --check-indexes=<true/false>%n" +
                        "                            Perform consistency checks on indexes.%n" +
                        "                              Default: true%n" +
                        "      --check-label-scan-store=<true/false>%n" +
                        "                            Perform consistency checks on the label scan store.%n" +
                        "                              Default: true%n" +
                        "      --check-property-owners=<true/false>%n" +
                        "                            Perform additional consistency checks on property%n" +
                        "                              ownership. This check is very expensive in time%n" +
                        "                              and memory.%n" +
                        "                              Default: false%n" +
                        "      --additional-config=<path>%n" +
                        "                            Configuration file to supply additional%n" +
                        "                              configuration in."
        ) ) );
    }

    @ParameterizedTest
    @ValueSource( ints = {5, 8} )
    void logRespectsTimeZone( int timezoneOffset ) throws IOException
    {
        // given
        TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( timezoneOffset ) ) );

        File cfg = dir.file( "neo4j.conf" );
        try ( PrintStream ps = new PrintStream( fs.openAsOutputStream( cfg, false ) ) )
        {
            ps.println( format( "%s=%s", db_timezone.name(), LogTimeZone.SYSTEM.name() ) );
        }

        // when
        String firstLogLine;
        try ( ByteArrayOutputStream os = new ByteArrayOutputStream(); PrintStream ps = new PrintStream( os ) )
        {
            ExecutionContext ctx = new ExecutionContext( dir.databaseDir().toPath(), cfg.getParentFile().toPath(), ps, ps, fs );

            new CommandLine( new OnlineBackupCommand( ctx ) ).execute(
                    "--verbose",
                    "--backup-dir", dir.directory( "backup" ).toString()
            ); //this backup will fail but first few lines will expose if log respects timezone.

            firstLogLine = os.toString().split( "\n", 1 )[0];
        }

        //then
        assertThat( firstLogLine, containsString( format( "+0%d00", timezoneOffset )) );
    }
}
