/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TimeZone;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.configuration.ssl.SslPolicyScope.FABRIC;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;

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
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim() ).isEqualTo( format(
                "Perform an online backup from a running Neo4j enterprise server.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "backup [--check-consistency] [--expand-commands] [--fallback-to-full]%n" +
                "       [--verbose] [--additional-config=<path>] --backup-dir=<path>%n" +
                "       [--check-graph=<true/false>] [--check-index-structure=<true/false>]%n" +
                "       [--check-indexes=<true/false>] [--check-label-scan-store=<true/false>]%n" +
                "       [--check-property-owners=<true/false>]%n" +
                "       [--check-relationship-type-scan-store=<true/false>]%n" +
                "       [--database=<database>] [--from=<host:port>]%n" +
                "       [--include-metadata=<all/users/roles>] [--pagecache=<size>]%n" +
                "       [--report-dir=<path>]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup%n" +
                "service must have been configured on the server beforehand.%n" +
                "%n" +
                "All consistency checks except 'cc-graph' can be quite expensive so it may be%n" +
                "useful to turn them off for very large databases. Increasing the heap size can%n" +
                "also be a good idea. See 'neo4j-admin help' for details.%n" +
                "%n" +
                "For more information see: https://neo4j.%n" +
                "com/docs/operations-manual/current/backup/%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose             Enable verbose output.%n" +
                "      --expand-commands     Allow command expansion in config value evaluation.%n" +
                "      --backup-dir=<path>   Directory to place backup in.%n" +
                "      --from=<host:port>    Host and port of Neo4j.%n" +
                "                              Default: localhost:6362%n" +
                "      --database=<database> Name of the remote database to backup. Can contain%n" +
                "                              * and ? for globbing.%n" +
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
                "      --check-index-structure=<true/false>%n" +
                "                            Perform structure checks on indexes.%n" +
                "                              Default: true%n" +
                "      --check-label-scan-store=<true/false>%n" +
                "                            Perform consistency checks on the label scan store.%n" +
                "                              Default: true%n" +
                "      --check-relationship-type-scan-store=<true/false>%n" +
                "                            Perform consistency checks on the relationship type%n" +
                "                              scan store.%n" +
                "                              Default: false%n" +
                "      --check-property-owners=<true/false>%n" +
                "                            Perform additional consistency checks on property%n" +
                "                              ownership. This check is very expensive in time%n" +
                "                              and memory.%n" +
                "                              Default: false%n" +
                "      --additional-config=<path>%n" +
                "                            Configuration file to supply additional%n" +
                "                              configuration in.%n" +
                "      --include-metadata=<all/users/roles>%n" +
                "                            Include metadata in file. Can't be used for backing%n" +
                "                              system database.%n" +
                "                            roles - commands to create the roles and privileges%n" +
                "                              (for both database and graph) that affect the use%n" +
                "                              of the database%n" +
                "                            users - commands to create the users that can use%n" +
                "                              the database and their role assignments%n" +
                "                            all - include roles and users"
        ) );
    }

    @ParameterizedTest
    @ValueSource( ints = {5, 8} )
    void logRespectsTimeZone( int timezoneOffset ) throws IOException
    {
        TimeZone defaultZone = TimeZone.getDefault();
        TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( timezoneOffset ) ) );
        try
        {
            String firstLogLine = executeBackup( DEFAULT_DATABASE_NAME );
            assertThat( firstLogLine ).contains( format( "+0%d00", timezoneOffset ) );
        }
        finally
        {
            TimeZone.setDefault( defaultZone );
        }
    }

    @Test
    void failOnIncorrectDatabaseName() throws IOException
    {
        String firstLogLine = executeBackup( randomAlphabetic( 2056 ) );
        assertThat( firstLogLine ).contains( "Invalid database name " );
    }

    @Test
    void ignoreUnrelatedSslPolicies() throws IOException
    {
        Map<Setting<?>,Object> additionalConfig = Map.of(
                SslPolicyConfig.forScope( BOLT ).enabled, true,
                SslPolicyConfig.forScope( HTTPS ).enabled, true,
                SslPolicyConfig.forScope( CLUSTER ).enabled, true,
                SslPolicyConfig.forScope( FABRIC ).enabled, true
        );

        String firstLogLine = executeBackup( DEFAULT_DATABASE_NAME, additionalConfig );
        // Since there is no server configured in this test, 'Connection refused' is the success path for tests here.
        // If the command got so far, it means that there was no error during the set up
        assertThat( firstLogLine ).contains( "Connection refused" );
    }

    private String executeBackup( String databaseName ) throws IOException
    {
        return executeBackup( databaseName, Map.of() );
    }

    private String executeBackup( String databaseName, Map<Setting<?>,Object> additionalConfig ) throws IOException
    {
        Path cfg = dir.file( "neo4j.conf" );
        try ( PrintStream ps = new PrintStream( fs.openAsOutputStream( cfg, false ) ) )
        {
            ps.printf( "%s=%s%n", db_timezone.name(), LogTimeZone.SYSTEM.name() );
            additionalConfig.forEach( ( key, value ) -> ps.printf( "%s=%s%n", key.name(), value ) );
        }

        // when
        try ( ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream( os );
                PrintWriter writer = new PrintWriter( ps ) )
        {
            ExecutionContext ctx = new ExecutionContext( dir.homePath(), cfg.getParent(), ps, ps, fs );

            String[] args = { "--verbose",
                              "--database", databaseName,
                              "--backup-dir", dir.directory( "backup" ).toString() };
            var command = new OnlineBackupCommand( ctx );
            new CommandLine( command ).setErr( writer ).execute( args );
            return os.toString().split( "%n", 1 )[0];
        }
    }
}
