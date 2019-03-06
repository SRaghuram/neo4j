/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class UnbindFromClusterCommandTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;

    private Path homeDir;
    private Path confDir;

    private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private FileChannel channel;

    @BeforeEach
    void setup()
    {
        homeDir = testDir.directory( "home" ).toPath();
        confDir = testDir.directory( "conf" ).toPath();
        fs.mkdir( homeDir.toFile() );

        when( outsideWorld.fileSystem() ).thenReturn( fs );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        IOUtils.closeAll( channel );
    }

    private File createClusterStateDir( FileSystemAbstraction fs )
    {
        File dataDir = new File( homeDir.toFile(), "data" );
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( fs, dataDir, false );
        clusterStateDirectory.initialize();
        return clusterStateDirectory.get();
    }

    @Test
    void shouldIgnoreIfSpecifiedDatabaseDoesNotExist() throws Exception
    {
        // given
        File clusterStateDir = createClusterStateDir( fs );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        // when
        command.execute( databaseNameParameter( "doesnotexist" ) );

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    void shouldFailToUnbindLiveDatabase() throws Exception
    {
        // given
        createClusterStateDir( fs );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        FileLock fileLock = createLockedFakeDbDir( homeDir );
        try
        {
            CommandFailed commandException = assertThrows( CommandFailed.class, () -> command.execute( databaseNameParameter( DEFAULT_DATABASE_NAME ) ) );
            assertThat( commandException.getMessage(), containsString( "Database is currently locked. Please shutdown Neo4j." ) );
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
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        // when
        command.execute( databaseNameParameter( DEFAULT_DATABASE_NAME ) );

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    void shouldReportWhenClusterStateDirectoryIsNotPresent() throws Exception
    {
        // given
        createUnlockedFakeDbDir( homeDir );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );
        command.execute( databaseNameParameter( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ) );
        verify( outsideWorld ).stdErrLine( "This instance was not bound. No work performed." );
    }

    @Test
    void shouldPrintUsage() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new UnbindFromClusterCommandProvider(), ps::println );

            assertThat( baos.toString(), containsString( "usage" ) );
        }
    }

    private void createUnlockedFakeDbDir( Path homeDir ) throws IOException
    {
        Path fakeDbDir = createFakeDbDir( homeDir );
        Files.createFile( DatabaseLayout.of( fakeDbDir.toFile() ).getStoreLayout().storeLockFile().toPath() );
    }

    private FileLock createLockedFakeDbDir( Path homeDir ) throws IOException
    {
        return createLockedStoreLockFileIn( createFakeDbDir( homeDir ) );
    }

    private Path createFakeDbDir( Path homeDir ) throws IOException
    {
        Path graphDb = homeDir.resolve( "data/databases/" + DEFAULT_DATABASE_NAME );
        fs.mkdirs( graphDb.toFile() );
        fs.create( graphDb.resolve( "neostore" ).toFile() ).close();
        return graphDb;
    }

    private FileLock createLockedStoreLockFileIn( Path databaseDir ) throws IOException
    {
        Path storeLockFile = Files.createFile( DatabaseLayout.of( databaseDir.toFile() ).getStoreLayout().storeLockFile().toPath() );
        channel = FileChannel.open( storeLockFile, READ, WRITE );
        return channel.lock( 0, Long.MAX_VALUE, true );
    }

    private static String[] databaseNameParameter( String databaseName )
    {
        return new String[]{"--database=" + databaseName};
    }
}
