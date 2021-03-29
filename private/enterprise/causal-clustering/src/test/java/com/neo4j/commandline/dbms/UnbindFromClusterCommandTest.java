/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static com.neo4j.configuration.CausalClusteringSettings.DEFAULT_CLUSTER_STATE_DIRECTORY_NAME;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_state_directory;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private static Stream<String> data()
    {
        return Stream.of( DEFAULT_DATA_DIR_NAME + "/" + DEFAULT_CLUSTER_STATE_DIRECTORY_NAME, "different_place" );
    }

    @BeforeEach
    void setup()
    {
        neo4jLayout = Neo4jLayout.of( testDir.homePath() );
        var homeDir = neo4jLayout.homeDirectory();
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
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Removes and archives all cluster state.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "unbind [--expand-commands] [--verbose] [--archive-cluster-state=<true/false>]%n" +
                "       [--archive-path=<path>]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Removes and archives all cluster state, so that the instance can rebind to a%n" +
                "cluster.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose           Enable verbose output.%n" +
                "      --expand-commands   Allow command expansion in config value evaluation.%n" +
                "      --archive-cluster-state=<true/false>%n" +
                "                          Enable or disable the cluster state archiving.%n" +
                "                            Default: false%n" +
                "      --archive-path=<path>%n" +
                "                          Destination (file or folder) of the cluster state%n" +
                "                            archive."
        ) ) ;
    }

    @Test
    void shouldFailToUnbindLiveDatabase() throws Exception
    {
        // given
        var command = new UnbindFromClusterCommand( ctx );

        var fileLock = createLockedFakeDbDir();
        try
        {
            var commandException = assertThrows( CommandFailedException.class, () ->
            {
                CommandLine.populateCommand( command );
                command.execute();
            } );
            assertThat( commandException.getMessage() ).contains( "Database is currently locked. Please shutdown database." ) ;
        }
        finally
        {
            fileLock.release();
        }
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void archiveTargetLocationDoesntExist( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();

        var command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command, "--archive-cluster-state=true", format( "--archive-path=%s", Path.of( "/location/doesnt/exist/" ).toString() ) );

        Exception exception = assertThrows( Exception.class, () -> command.execute() );

        // then
        String expectedMessage = "Can't write the archive because the destination directory is not writable.";
        String actualMessage = exception.getMessage();

        assertTrue( actualMessage.contains( expectedMessage ) );
        assertTrue( fs.fileExists( clusterStateDir ) );
        assertTrue( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldFailWhenUserSpecifiedArchiveAlreadyExists( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        var userSpecifiedArchiveFile = createUserSpecifiedArchiveDirectory().resolve( "archive.zip" );
        Files.createFile( userSpecifiedArchiveFile );

        var command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command, "--archive-cluster-state=true", format( "--archive-path=%s", userSpecifiedArchiveFile.toString() ) );
        Exception exception = assertThrows( Exception.class, () -> command.execute() );

        // then
        String expectedMessage = "Archive already exists.";
        String actualMessage = exception.getMessage();

        assertTrue( actualMessage.contains( expectedMessage ) );
        assertTrue( fs.fileExists( clusterStateDir ) );
        assertTrue( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldFailWhenArchiveAlreadyExists( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        Clock fakeClock = Clocks.fakeClock();
        createFakeDefaultArchive( fakeClock );

        var command = new UnbindFromClusterCommand( ctx, fakeClock );
        CommandLine.populateCommand( command, "--archive-cluster-state=true" );

        // when
        Exception exception = assertThrows( Exception.class, () -> command.execute() );

        // then
        String expectedMessage = "Archive already exists.";
        String actualMessage = exception.getMessage();

        assertTrue( actualMessage.contains( expectedMessage ) );
        assertTrue( fs.fileExists( clusterStateDir ) );
        assertTrue( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldWriteArchiveAtSpecifiedAbsoluteLocation( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        var userSpecifiedArchiveFile = createUserSpecifiedArchiveDirectory().resolve( "archive.zip" );

        Set<byte[]> expected = getBytesOfFilesIn( List.of( clusterStateDir, serverIdStore ) );

        var command = new UnbindFromClusterCommand( ctx );
        CommandLine.populateCommand( command, "--archive-cluster-state=true" );

        // when
        CommandLine.populateCommand( command, format( "--archive-path=%s", userSpecifiedArchiveFile.toString() ) );
        command.execute();

        // then
        Set<byte[]> actual = getBytesOfFilesIn( userSpecifiedArchiveFile );

        assertTrue( Files.exists( userSpecifiedArchiveFile ) );
        assertThat( expected ).containsExactlyInAnyOrderElementsOf( actual );
        assertFalse( fs.fileExists( clusterStateDir ) );
        assertFalse( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldWriteArchiveAtSpecifiedLocation( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        var userSpecifiedArchiveDirectory = createUserSpecifiedArchiveDirectory();
        FakeClock fakeClock = Clocks.fakeClock();
        Set<byte[]> expected = getBytesOfFilesIn( List.of( clusterStateDir, serverIdStore ) );

        var command = new UnbindFromClusterCommand( ctx, fakeClock );

        // when
        CommandLine.populateCommand( command, "--archive-cluster-state=true", format( "--archive-path=%s", userSpecifiedArchiveDirectory.toString() ) );
        command.execute();

        // then
        Path archive = userSpecifiedArchiveDirectory.resolve( getArchiveNameUsingClock( fakeClock ) );
        Set<byte[]> actual = getBytesOfFilesIn( archive );

        assertTrue( archiveIsPresentIn( userSpecifiedArchiveDirectory ) );
        assertThat( expected ).containsExactlyInAnyOrderElementsOf( actual );
        assertFalse( fs.fileExists( clusterStateDir ) );
        assertFalse( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldArchiveClusterEvenIfStoreIdDoesntExist( String clusterStateDirectory ) throws Exception
    {
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        FakeClock fakeClock = Clocks.fakeClock();
        Set<byte[]> expected = getBytesOfFilesIn( List.of( clusterStateDir ) );

        var command = new UnbindFromClusterCommand( ctx, fakeClock );

        // when
        CommandLine.populateCommand( command, format( "--archive-cluster-state=true" ) );
        command.execute();

        // then
        Path archive = ctx.homeDir().resolve( getArchiveNameUsingClock( fakeClock ) );
        Set<byte[]> actual = getBytesOfFilesIn( archive );

        assertTrue( archiveIsPresentIn( ctx.homeDir() ) );
        assertThat( expected ).containsExactlyInAnyOrderElementsOf( actual );
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldArchiveClusterStateDirectoryAndServerId( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        FakeClock fakeClock = Clocks.fakeClock();
        Set<byte[]> expected = getBytesOfFilesIn( List.of( clusterStateDir, serverIdStore ) );

        var command = new UnbindFromClusterCommand( ctx, fakeClock );

        // when
        CommandLine.populateCommand( command, format( "--archive-cluster-state=true" ) );
        command.execute();

        // then
        Path archive = ctx.homeDir().resolve( getArchiveNameUsingClock( fakeClock ) );
        Set<byte[]> actual = getBytesOfFilesIn( archive );

        assertTrue( archiveIsPresentIn( ctx.homeDir() ) );
        assertThat( expected ).containsExactlyInAnyOrderElementsOf( actual );
        assertFalse( fs.fileExists( clusterStateDir ) );
        assertFalse( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldNotArchiveClusterStateDirectoryAndServerId( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        var command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command, format( "--archive-cluster-state=false" ) );
        command.execute();

        // then
        assertFalse( archiveIsPresentIn( ctx.homeDir() ) );
        assertFalse( fs.fileExists( clusterStateDir ) );
        assertFalse( fs.fileExists( serverIdStore ) );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldRemoveClusterStateDirectoryAndServerId( String clusterStateDirectory ) throws Exception
    {
        // given
        createUnlockedFakeDbDir();
        var clusterStateDir = createClusterStateDirAndSetInConfig( clusterStateDirectory );
        var serverIdStore = createServerIdStore();
        var command = new UnbindFromClusterCommand( ctx );

        // when
        CommandLine.populateCommand( command );
        command.execute();

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
        assertFalse( fs.fileExists( serverIdStore ) );
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

    private Path createUserSpecifiedArchiveDirectory() throws IOException
    {
        var userArchiveDir = testDir.homePath().resolve( "user_specified_archive_dir" );
        fs.mkdirs( userArchiveDir );
        return userArchiveDir;
    }

    private String getArchiveNameUsingClock( Clock clock )
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "yyyyMMddHHmmss")
                                                       .withZone( ZoneId.systemDefault());
        String timestamp = formatter.format( clock.instant() );
        String fileName = format( "unbound_cluster_state.%s.zip", timestamp );
        return fileName;
    }

    private Path createFakeDefaultArchive( Clock clock ) throws IOException
    {
        String fileName = getArchiveNameUsingClock( clock );
        Path archivePath = ctx.homeDir().resolve( fileName );
        Files.createFile( archivePath );
        return archivePath;
    }

    private boolean archiveIsPresentIn( Path path ) throws IOException
    {
        return Files.find( path,
                    1,
                    ( p, a ) -> {
                        File file = p.toFile();
                        return file.isFile() && file.getName().startsWith( "unbound_cluster_state" );
                    } )
             .count() == 1;
    }

    private Path createClusterStateDirAndSetInConfig( String def ) throws IOException
    {
        var path = neo4jLayout.homeDirectory().resolve( def );
        var clusterStateDirectory = ClusterStateLayout.of( path ).getClusterStateDirectory();
        fs.mkdirs( clusterStateDirectory );
        FileUtils.writeStringToFile(  new File( clusterStateDirectory.toFile(), "fake_file" )  , "fake content", Charset.defaultCharset() );

        var setting = format( "%s=%s", cluster_state_directory.name(), path.toString().replace( '\\', '/' ) );
        fs.mkdirs( ctx.confDir() );
        Files.write( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ), Collections.singleton( setting ) );

        return clusterStateDirectory;
    }

    private Path createServerIdStore() throws IOException
    {
        var serverIdFile = neo4jLayout.serverIdFile();
        try ( var channel = fs.write( serverIdFile ) )
        {
            channel.writeAll( ByteBuffer.wrap( new byte[]{0} ) );
        }
        return serverIdFile;
    }

    private void createUnlockedFakeDbDir() throws IOException
    {
        createLockedFakeDbDir().release();
    }

    private FileLock createLockedFakeDbDir() throws IOException
    {
        Files.createDirectories( neo4jLayout.databasesDirectory() );
        channel = fs.write( neo4jLayout.storeLockFile() );
        var fileLock = channel.tryLock();
        assertNotNull( fileLock, "Unable to acquire a store lock" );
        return fileLock;
    }

    private Set<byte[]> getBytesOfFilesIn( Collection<Path> paths ) throws IOException
    {
        Set<byte[]> bytesSet = new HashSet<>();

        SimpleFileVisitor<Path> simpleFileVisitor = new SimpleFileVisitor<>()
        {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
                    throws IOException
            {
                byte[] bytes = FileUtils.readFileToByteArray( file.toFile() );
                bytesSet.add( bytes );
                return FileVisitResult.CONTINUE;
            }
        };

        for ( Path path : paths )
        {
            Files.walkFileTree( path, simpleFileVisitor );
        }
        return bytesSet;
    }

    private Set<byte[]> getBytesOfFilesIn( Path archive ) throws IOException
    {
        Set<byte[]> bytesSet = new HashSet<>();

        try ( ZipInputStream zis = new ZipInputStream( new FileInputStream( archive.toFile() ) ) )
        {
            ZipEntry zipEntry = zis.getNextEntry();

            while ( zipEntry != null )
            {
                if ( !zipEntry.isDirectory() )
                {
                    byte[] allBytes = zis.readAllBytes();

                    bytesSet.add( allBytes );
                }
                zipEntry = zis.getNextEntry();
            }
        }
        return  bytesSet;
    }

}
