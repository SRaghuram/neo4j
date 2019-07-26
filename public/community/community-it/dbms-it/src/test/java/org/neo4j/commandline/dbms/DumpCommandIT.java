/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.LayoutConfig;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.dbms.archive.CompressionFormat.ZSTD;

@ExtendWith( TestDirectoryExtension.class )
class DumpCommandIT
{
    @Inject
    private TestDirectory testDirectory;

    private Path homeDir;
    private Path configDir;
    private Path archive;
    private Dumper dumper;
    private Path databaseDirectory;

    @BeforeEach
    void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" ).toPath();
        configDir = testDirectory.directory( "config-dir" ).toPath();
        archive = testDirectory.file( "some-archive.dump" ).toPath();
        dumper = mock( Dumper.class );
        putStoreInDirectory( buildConfig(), homeDir.resolve( "data/databases/foo" ) );
        databaseDirectory = homeDir.resolve( "data/databases/foo" );
    }

    private Config buildConfig()
    {
        Config config = Config.newBuilder()
                .fromFileNoThrow( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .set( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath().toString() )
                .build();
        ConfigUtils.disableAllConnectors( config );
        return config;
    }

    @Test
    void shouldDumpTheDatabaseToTheArchive() throws Exception
    {
        execute( "foo" );
        verify( dumper ).dump( eq( homeDir.resolve( "data/databases/foo" ) ),
                eq( homeDir.resolve( "data/tx-logs/foo" ) ), eq( archive ), eq( ZSTD ), any() );
    }

    @Test
    void shouldCalculateTheDatabaseDirectoryFromConfig() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path txLogsDir = dataDir.resolve( DEFAULT_TX_LOGS_ROOT_DIR_NAME + "/foo" );
        Path databaseDir = dataDir.resolve( "databases/foo" );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ), singletonList( formatProperty( data_directory, dataDir ) ) );
        putStoreInDirectory( buildConfig(), databaseDir );

        execute( "foo" );
        verify( dumper ).dump( eq( databaseDir ), eq( txLogsDir ), any(), any(), any() );
    }

    @Test
    void shouldCalculateTheTxLogDirectoryFromConfig() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path txlogsRoot = testDirectory.directory( "txLogsPath" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo" );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                asList( formatProperty( data_directory, dataDir ),
                        formatProperty( transaction_logs_root_path, txlogsRoot ) ) );
        putStoreInDirectory( buildConfig(), databaseDir );

        execute( "foo" );
        verify( dumper ).dump( eq( databaseDir ), eq( txlogsRoot.resolve( "foo" ) ), any(), any(), any() );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldHandleDatabaseSymlink() throws Exception
    {
        Path realDatabaseDir = testDirectory.directory( "path-to-links/foo" ).toPath();

        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo" );
        Path txLogsDir = dataDir.resolve( DEFAULT_TX_LOGS_ROOT_DIR_NAME + "/foo" );

        Files.createDirectories( dataDir.resolve( "databases" ) );

        Files.createSymbolicLink( databaseDir, realDatabaseDir );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                singletonList( format( "%s=%s", data_directory.name(), dataDir.toString().replace( '\\', '/' ) ) ) );
        putStoreInDirectory( buildConfig(), realDatabaseDir );

        execute( "foo" );
        verify( dumper ).dump( eq( realDatabaseDir ), eq( txLogsDir ), any(), any(), any() );
    }

    @Test
    void shouldCalculateTheArchiveNameIfPassedAnExistingDirectory() throws Exception
    {
        File to = testDirectory.directory( "some-dir" );
        execute( "foo", to.toPath() );
        verify( dumper ).dump( any( Path.class ), any( Path.class ), eq( to.toPath().resolve( "foo.dump" ) ), any(), any() );
    }

    @Test
    void shouldNotCalculateTheArchiveNameIfPassedAnExistingFile()
            throws Exception
    {
        Files.createFile( archive );
        execute( "foo" );
        verify( dumper ).dump( any(), any(), eq( archive ), any(), any() );
    }

    @Test
    void shouldRespectTheStoreLock() throws Exception
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo" );
        StoreLayout storeLayout = DatabaseLayout.of( databaseDirectory.toFile() ).getStoreLayout();
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem, storeLayout ) )
        {
            storeLocker.checkLock();

            CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
            assertEquals( "The database is in use. Stop Neo4j and try again.", commandFailed.getMessage() );
        }
    }

    @Test
    void databaseThatRequireRecoveryIsNotDumpable() throws IOException
    {
        Config config = Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir.toString() );
        DatabaseLayout databaseLayout = testDirectory.databaseLayout( "foo", LayoutConfig.of( config ) );
        testDirectory.getFileSystem().mkdirs( databaseLayout.getTransactionLogsDirectory() );
        File logFile = new File( databaseLayout.getTransactionLogsDirectory(), TransactionLogFilesHelper.DEFAULT_NAME + ".0" );

        try ( FileWriter fileWriter = new FileWriter( logFile ) )
        {
            fileWriter.write( "brb" );
        }
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
        assertThat( commandFailed.getMessage(), startsWith( "Active logical log detected, this might be a source of inconsistencies." ) );
    }

    @Test
    void shouldReleaseTheStoreLockAfterDumping() throws Exception
    {
        execute( "foo" );
        assertCanLockStore( databaseDirectory );
    }

    @Test
    void shouldReleaseTheStoreLockEvenIfThereIsAnError() throws Exception
    {
        doThrow( IOException.class ).when( dumper ).dump( any(), any(), any(), any(), any() );
        assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
        assertCanLockStore( databaseDirectory );
    }

    @Test
    void shouldNotAccidentallyCreateTheDatabaseDirectoryAsASideEffectOfStoreLocking()
            throws Exception
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/accident" );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( databaseDirectory ), equalTo( false ) );
            return null;
        } ).when( dumper ).dump( any(), any(), any(), any(), any() );

        execute( "foo" );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldReportAHelpfulErrorIfWeDontHaveWritePermissionsForLock() throws Exception
    {
        StoreLayout storeLayout = DatabaseLayout.of( databaseDirectory.toFile() ).getStoreLayout();
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem, storeLayout ) )
        {
            storeLocker.checkLock();

            try ( Closeable ignored = withPermissions( storeLayout.storeLockFile().toPath(), emptySet() ) )
            {
                CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
                assertEquals( "You do not have permission to dump the database. Is Neo4j running as a different user?", commandFailed.getMessage() );
            }
        }
    }

    @Test
    void shouldExcludeTheStoreLockFromTheArchiveToAvoidProblemsWithReadingLockedFilesOnWindows()
            throws Exception
    {
        File lockFile = StoreLayout.of( new File( "." ) ).storeLockFile();
        doAnswer( invocation ->
        {
            Predicate<Path> exclude = invocation.getArgument( 4 );
            assertThat( exclude.test( Paths.get( lockFile.getName() ) ), is( true ) );
            assertThat( exclude.test( Paths.get( "some-other-file" ) ), is( false ) );
            return null;
        } ).when( dumper ).dump(any(), any(), any(), any(), any() );

        execute( "foo" );
    }

    @Test
    void shouldDefaultToGraphDB() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path txLogsDir = dataDir.resolve( DEFAULT_TX_LOGS_ROOT_DIR_NAME + "/" + DEFAULT_DATABASE_NAME );
        Path databaseDir = dataDir.resolve( "databases/" + DEFAULT_DATABASE_NAME );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ), singletonList( formatProperty( data_directory, dataDir ) ) );
        putStoreInDirectory( buildConfig(), databaseDir );

        execute( DEFAULT_DATABASE_NAME );
        verify( dumper ).dump( eq( databaseDir ), eq( txLogsDir ), any(), any(), any() );
    }

    @Test
    void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws Exception
    {
        doThrow( new FileAlreadyExistsException( "the-archive-path" ) ).when( dumper ).dump( any(), any(), any(), any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
        assertEquals( "Archive already exists: the-archive-path", commandFailed.getMessage() );
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabaseDoesntExist()
    {
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "bobo" ) );
        assertEquals( "Database does not exist: bobo", commandFailed.getMessage() );
    }

    @Test
    void shouldGiveAClearMessageIfTheArchivesParentDoesntExist() throws Exception
    {
        doThrow( new NoSuchFileException( archive.getParent().toString() ) ).when( dumper ).dump(any(), any(), any(), any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
        assertEquals( "Unable to dump database: NoSuchFileException: " + archive.getParent(), commandFailed.getMessage() );
    }

    @Test
    void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws Exception
    {
        doThrow( new IOException( "the-message" ) ).when( dumper ).dump(any(), any(), any(), any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo" ) );
        assertEquals( "Unable to dump database: IOException: the-message", commandFailed.getMessage() );
    }

    private void execute( String database )
    {
        execute( database, archive );
    }

    private void execute( String database, Path to )
    {
        final ExecutionContext ctx = new ExecutionContext( homeDir, configDir, mock( PrintStream.class ), mock( PrintStream.class ),
                testDirectory.getFileSystem() );
        final var command = new DumpCommand( ctx, dumper );

        CommandLine.populateCommand( command,
                "--database=" + database,
                "--to=" + to.toAbsolutePath() );

        command.execute();
    }

    private static void assertCanLockStore( Path databaseDirectory ) throws IOException
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem, DatabaseLayout.of( databaseDirectory.toFile() ).getStoreLayout() ) )
        {
            storeLocker.checkLock();
        }
    }

    private void putStoreInDirectory( Config config, Path databaseDirectory )
    {
        String databaseName = databaseDirectory.toFile().getName();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseDirectory.getParent().getParent().getParent().toFile() )
                .setConfig( config )
                .setConfig( default_database, databaseName )
                .setConfig( databases_root_path, databases_root_path.defaultValue().toString() )
                .build();
        managementService.shutdown();
    }

    private static Closeable withPermissions( Path file, Set<PosixFilePermission> permissions ) throws IOException
    {
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions( file );
        Files.setPosixFilePermissions( file, permissions );
        return () -> Files.setPosixFilePermissions( file, originalPermissions );
    }

    private static String formatProperty( Setting setting, Path path )
    {
        return format( "%s=%s", setting.name(), path.toString().replace( '\\', '/' ) );
    }
}
