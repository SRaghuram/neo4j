/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class UnbindFromClusterCommandTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private FileSystemAbstraction fs;

    private Path homeDir;
    private Path confDir;
    private PrintStream err;
    private ExecutionContext ctx;

    private FileChannel channel;

    @BeforeEach
    void setup()
    {
        homeDir = testDir.directory( "home" ).toPath();
        confDir = testDir.directory( "conf" ).toPath();
        fs.mkdir( homeDir.toFile() );
        err = mock( PrintStream.class );
        ctx = new ExecutionContext( homeDir, confDir, System.out, err, fs );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        IOUtils.closeAll( channel );
    }

    private File createClusterStateDir( FileSystemAbstraction fs ) throws IOException
    {
        File dataDir = new File( homeDir.toFile(), "data" );
        File clusterStateDirectory = ClusterStateLayout.of( dataDir ).getClusterStateDirectory();
        fs.mkdirs( clusterStateDirectory );
        return clusterStateDirectory;
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new UnbindFromClusterCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                "Removes cluster state data for the specified database.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "unbind [--verbose] [--database=<database>]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Removes cluster state data for the specified database, so that the instance can%n" +
                "rebind to a new or recovered cluster.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose   Enable verbose output.%n" +
                "      --database=<database>%n" +
                "                  Name of the database.%n" +
                "                    Default: neo4j"
        ) ) );
    }

    @Test
    void shouldIgnoreIfSpecifiedDatabaseDoesNotExist() throws Exception
    {
        // given
        File clusterStateDir = createClusterStateDir( fs );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command, databaseNameParameter( "doesnotexist" ) );
        command.execute();

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    void shouldFailToUnbindLiveDatabase() throws Exception
    {
        // given
        createClusterStateDir( fs );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( ctx );

        FileLock fileLock = createLockedFakeDbDir( homeDir );
        try
        {
            CommandFailedException commandException = assertThrows( CommandFailedException.class, () ->
            {
                CommandLine.populateCommand( command, databaseNameParameter( DEFAULT_DATABASE_NAME ) );
                command.execute();
            } );
            assertThat( commandException.getMessage(), containsString( "Database is currently locked. Please shutdown database." ) );
        }
        finally
        {
            fileLock.release();
        }
    }

    @Test
    void shouldRemoveClusterStateDirectoryForGivenDatabase() throws Exception
    {
        // given
        File clusterStateDir = createClusterStateDir( fs );
        createUnlockedFakeDbDir( homeDir );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command, databaseNameParameter( DEFAULT_DATABASE_NAME ) );
        command.execute();

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    void shouldReportWhenClusterStateDirectoryIsNotPresent() throws Exception
    {
        // given
        createUnlockedFakeDbDir( homeDir );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( ctx );
        CommandLine.populateCommand( command, databaseNameParameter( DEFAULT_DATABASE_NAME ) );
        command.execute();

        verify( err ).println( "This instance was not bound. No work performed." );
    }

    private void createUnlockedFakeDbDir( Path homeDir ) throws IOException
    {
        Path fakeDbDir = createFakeDbDir( homeDir );
        Files.createFile( DatabaseLayout.ofFlat( fakeDbDir.toFile() ).getNeo4jLayout().storeLockFile().toPath() );
    }

    private FileLock createLockedFakeDbDir( Path homeDir ) throws IOException
    {
        return createLockedStoreLockFileIn( createFakeDbDir( homeDir ) );
    }

    private Path createFakeDbDir( Path homeDir ) throws IOException
    {
        Path graphDb = homeDir.resolve( "data/databases/" + DEFAULT_DATABASE_NAME );
        fs.mkdirs( graphDb.toFile() );
        fs.write( graphDb.resolve( "neostore" ).toFile() ).close();
        return graphDb;
    }

    private FileLock createLockedStoreLockFileIn( Path databaseDir ) throws IOException
    {
        Path storeLockFile = Files.createFile( DatabaseLayout.ofFlat( databaseDir.toFile() ).databaseLockFile().toPath() );
        channel = FileChannel.open( storeLockFile, READ, WRITE );
        return channel.lock( 0, Long.MAX_VALUE, true );
    }

    private static String[] databaseNameParameter( String databaseName )
    {
        return new String[]{"--database=" + databaseName};
    }
}
