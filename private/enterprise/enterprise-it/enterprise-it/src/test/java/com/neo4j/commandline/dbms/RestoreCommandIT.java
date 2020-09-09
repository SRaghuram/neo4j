/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.restore.RestoreDatabaseCli;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.Neo4jLayout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RestoreCommandIT extends AbstractCommandIT
{
    @Test
    void failToRestoreRunningDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        Path testBackup = testDirectory.directoryPath( "testbackup" );
        FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), testBackup );
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String fromPath = testBackup.toAbsolutePath().toString();
                                                             restoreDatabase( Optional.of( databaseName ), fromPath, Optional.empty(),
                                                                              Optional.empty(), Optional.empty() );
                                                         } );
        assertThat( exception.getCause().getMessage() ).startsWith( "The database is in use. Stop database" );
    }

    @Test
    void restoreStoppedDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        managementService.shutdownDatabase( databaseName );
        Path testBackup = testDirectory.directoryPath( "testbackup2" );
        FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), testBackup );

        assertDoesNotThrow( () ->
                            {
                                final String fromPath = testBackup.toAbsolutePath().toString();
                                restoreDatabase( Optional.of( databaseName ), fromPath, Optional.empty(), Optional.empty(),
                                                 Optional.empty() );
                            } );
    }

    @Test
    void shouldUseLastNameOfFromPathIfDatabaseNameParameterIsNotPassed() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        Path testBackup = testDirectory.directoryPath( "testbackup2" );
        FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), testBackup );

        assertDoesNotThrow( () ->
                            {
                                final String fromPath = testBackup.toAbsolutePath().toString();
                                restoreDatabase( Optional.empty(), fromPath, Optional.empty(), Optional.empty(), Optional.empty() );
                            } );

        final String databaseName = testBackup.getName( testBackup.getNameCount() - 1 ).toString();
        final Path restoredDatabaseFolder = Neo4jLayout.of( config ).databaseLayout( databaseName ).databaseDirectory();

        assertThat( restoredDatabaseFolder.toFile() ).isDirectory();
        assertThat( restoredDatabaseFolder.toFile().listFiles() ).isNotEmpty();
    }

    @Test
    void shouldRestoreTwoDatabaseDefinedAsAList() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        final List<Path> databaseDirs = List.of( testDirectory.directoryPath( "db1", "testDir" ),
                                                 testDirectory.directoryPath( "db2", "testDir" ) );
        databaseDirs.forEach( dir -> copy( databaseAPI.databaseLayout().databaseDirectory(), dir ) );

        //when
        assertDoesNotThrow( () ->
                            {
                                final String fromPath = databaseDirs.stream()
                                                                    .map( dir -> dir.toAbsolutePath().toString() )
                                                                    .collect( Collectors.joining( "," ) );
                                restoreDatabase( Optional.empty(), fromPath, Optional.empty(), Optional.empty(), Optional.empty() );
                            } );

        //then db1 and db2 are created
        databaseDirs.stream()
                    .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
                    .map( path -> Neo4jLayout.of( config ).databaseLayout( path ).databaseDirectory() )
                    .forEach( restoredDatabaseFolder ->
                              {
                                  assertThat( restoredDatabaseFolder.toFile() ).isDirectory();
                                  assertThat( restoredDatabaseFolder.toFile().listFiles() ).isNotEmpty();
                              } );
    }

    @Test
    void shouldRestoreAllDatabasesThatMatchTheFilter() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        final List<Path> databaseDirs = List.of( testDirectory.directoryPath( "db1", "testDir" ),
                                                 testDirectory.directoryPath( "db2", "testDir" ),
                                                 testDirectory.directoryPath( "mongo", "testDir" ) );
        databaseDirs.forEach( dir -> copy( databaseAPI.databaseLayout().databaseDirectory(), dir ) );

        var fromPath = databaseDirs.get( 0 ).getParent().toAbsolutePath().toString(); // point to testDir folder

        //when
        assertDoesNotThrow( () -> restoreDatabase( Optional.empty(), fromPath, Optional.empty(), Optional.of( "d*" ), Optional.empty() ) );

        //then db1 and db2 are created
        databaseDirs.subList( 0, 2 ).stream()
                    .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
                    .map( path -> Neo4jLayout.of( config ).databaseLayout( path ).databaseDirectory() )
                    .forEach( restoredDatabaseFolder ->
                              {
                                  assertThat( restoredDatabaseFolder.toFile() ).isDirectory();
                                  assertThat( restoredDatabaseFolder.toFile().listFiles() ).isNotEmpty();
                              } );

        //then mongo is not created because doesn't match the pattern
        List.of( databaseDirs.get( 2 ) ).stream()
            .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
            .map( path -> Neo4jLayout.of( config ).databaseLayout( path ).databaseDirectory() )
            .forEach( notRestoredDB -> assertThat( notRestoredDB.toFile() ).doesNotExist() );
    }

    @Test
    void shouldNotThrowExceptionWhenMultiDatabaseAreRestoreWithMoveFlag()
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        final List<Path> databaseDirs = List.of( testDirectory.directoryPath( "db1", "testDir" ),
                                                 testDirectory.directoryPath( "db2", "testDir" ) );
        databaseDirs.forEach( dir -> copy( databaseAPI.databaseLayout().databaseDirectory(), dir ) );

        var homeDir = testDirectory.directoryPath( "home", "restoreHome" );

        //when
        assertDoesNotThrow( () ->
                            {
                                final String fromPath = databaseDirs.stream()
                                                                    .map( dir -> dir.toAbsolutePath().toString() )
                                                                    .collect( Collectors.joining( "," ) );
                                restoreDatabase( Optional.empty(), fromPath, Optional.of( homeDir ), Optional.empty(), Optional.of( true ) );
                            } );

        //then db1 and db2 are created
        databaseDirs.stream()
                    .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
                    .map( path -> Neo4jLayout.of( homeDir ).databaseLayout( path ).databaseDirectory() )
                    .forEach( restoredDatabaseFolder ->
                              {
                                  assertThat( restoredDatabaseFolder.toFile() ).isDirectory();
                                  assertThat( restoredDatabaseFolder.toFile().listFiles() ).isNotEmpty();
                              } );
    }

    @Test
    void throwExceptionWhenBothDatabaseAndFilterAreDefined()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String fromPath = Path.of( "/root" ).toAbsolutePath().toString();
                                                             restoreDatabase( Optional.of( "test" ), fromPath, Optional.empty(),
                                                                              Optional.of( "t*" ), Optional.empty() );
                                                         } );

        assertThat( exception.getMessage() ).contains( "Can't define database name and filter parameters together" );
    }

    @Test
    void throwExceptionWhenFromContainsListOfValuesAndDatabaseParameterIsDefined()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String first = Path.of( "/root" ).toAbsolutePath().toString();
                                                             final String second = Path.of( "/system" ).toAbsolutePath().toString();

                                                             restoreDatabase( Optional.of( "test" ), first + "," + second, Optional.empty(),
                                                                              Optional.empty(), Optional.empty() );
                                                         } );

        assertThat( exception.getMessage() ).contains( "Database parameter can be applied only when --fromPaths is single value" );
    }

    @Test
    void throwExceptionWhenFromPathPointToTheRootOfTheFileSystem()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String fromPath = Path.of( "/" ).toAbsolutePath().toString();
                                                             restoreDatabase( Optional.empty(), fromPath, Optional.empty(),
                                                                              Optional.empty(), Optional.empty() );
                                                         } );
        assertThat( exception.getMessage() ).contains( "Error in executing restore for path" );
    }

    private void restoreDatabase( Optional<String> database, String fromPath, Optional<Path> homeDirectory,
                                  Optional<String> filter, Optional<Boolean> move )
            throws IOException
    {
        var command = new RestoreDatabaseCli( getExtensionContext() );

        String[] args = buildArgs( database, fromPath, homeDirectory, filter, move );
        CommandLine.populateCommand( command, args );

        command.execute();
    }

    private String[] buildArgs( Optional<String> database,
                                String fromPath,
                                Optional<Path> homeDirectory,
                                Optional<String> filter,
                                Optional<Boolean> move )
    {
        StringJoiner args = new StringJoiner( "!!!" );
        database.ifPresent( v -> args.add( "--database=" + v ) );
        args.add( "--from=" + fromPath );
        homeDirectory.ifPresent( v -> args.add( "--neo4j-home-directory=" + v ) );
        filter.ifPresent( v -> args.add( "--filter=" + v ) );
        move.ifPresent( v -> args.add( "--move" ) );

        args.add( "--force" );

        return args.toString().split( "!!!" );
    }

    private void copy( Path from, Path to )
    {
        try
        {
            FileUtils.copyDirectory( from, to );
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
}
