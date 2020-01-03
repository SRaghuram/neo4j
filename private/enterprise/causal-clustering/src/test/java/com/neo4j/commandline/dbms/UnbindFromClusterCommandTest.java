/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.nio.channels.FileLock;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;

@TestDirectoryExtension
class UnbindFromClusterCommandTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private FileSystemAbstraction fs;

    private Neo4jLayout neo4jLayout;
    private PrintStream err;
    private ExecutionContext ctx;

    private StoreChannel channel;

    @BeforeEach
    void setup()
    {
        neo4jLayout = Neo4jLayout.of( testDir.homeDir() );
        var homeDir = neo4jLayout.homeDirectory().toPath();
        var confDir = homeDir.resolve( "conf" );

        err = mock( PrintStream.class );
        ctx = new ExecutionContext( homeDir, confDir, System.out, err, fs );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        IOUtils.closeAll( channel );
    }

    @Test
    void printUsageHelp()
    {
        var baos = new ByteArrayOutputStream();
        var command = new UnbindFromClusterCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                "Removes cluster state data for the specified database.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "unbind [--verbose]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Removes cluster state data for the specified database, so that the instance can%n" +
                "rebind to a new or recovered cluster.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose   Enable verbose output."
        ) ) );
    }

    @Test
    void shouldFailToUnbindLiveDatabase() throws Exception
    {
        // given
        createClusterStateDir();
        var command = new UnbindFromClusterCommand( ctx );

        var fileLock = createLockedFakeDbDir();
        try
        {
            var commandException = assertThrows( CommandFailedException.class, () ->
            {
                CommandLine.populateCommand( command );
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
    void shouldRemoveClusterStateDirectory() throws Exception
    {
        // given
        var clusterStateDir = createClusterStateDir();
        createUnlockedFakeDbDir();
        var command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command );
        command.execute();

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    void shouldReportWhenClusterStateDirectoryIsNotPresent() throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var command = new UnbindFromClusterCommand( ctx );
        CommandLine.populateCommand( command );
        command.execute();

        verify( err ).println( "This instance was not bound. No work performed." );
    }

    private File createClusterStateDir() throws IOException
    {
        var dataDir = neo4jLayout.homeDirectory().toPath().resolve( DEFAULT_DATA_DIR_NAME );
        var clusterStateDirectory = ClusterStateLayout.of( dataDir.toFile() ).getClusterStateDirectory();
        fs.mkdirs( clusterStateDirectory );
        return clusterStateDirectory;
    }

    private void createUnlockedFakeDbDir() throws IOException
    {
        createLockedFakeDbDir().release();
    }

    private FileLock createLockedFakeDbDir() throws IOException
    {
        fs.mkdirs( neo4jLayout.databasesDirectory() );
        channel = fs.write( neo4jLayout.storeLockFile() );
        var fileLock = channel.tryLock();
        assertNotNull( fileLock, "Unable to acquire a store lock" );
        return fileLock;
    }
}
