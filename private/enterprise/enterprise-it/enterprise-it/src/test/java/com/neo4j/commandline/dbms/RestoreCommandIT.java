/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.restore.RestoreDatabaseCli;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.Neo4jLayout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static picocli.CommandLine.ParameterException;
import static picocli.CommandLine.populateCommand;

class RestoreCommandIT extends AbstractCommandIT
{
    @Test
    void failToRestoreRunningDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        Path testBackup = testDirectory.directory( "testbackup" );
        FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), testBackup );
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String fromPath = testBackup.toAbsolutePath().toString();
                                                             restoreDatabase( Optional.of( databaseName ), fromPath,
                                                                              Optional.empty(), Optional.empty(), Optional.empty() );
                                                         } );
        assertThat( exception.getCause().getMessage() ).startsWith( "The database is in use. Stop database" );
    }

    @Test
    void restoreStoppedDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        managementService.shutdownDatabase( databaseName );
        Path testBackup = testDirectory.directory( "testbackup2" );
        FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), testBackup );

        assertDoesNotThrow( () ->
                            {
                                final String fromPath = testBackup.toAbsolutePath().toString();
                                restoreDatabase( Optional.of( databaseName ), fromPath,
                                                 Optional.empty(), Optional.empty(), Optional.empty() );
                            } );
    }

    @Test
    void shouldUseLastNameOfFromPathIfDatabaseNameParameterIsNotPassed() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        Path testBackup = testDirectory.directory( "testbackup2" );
        FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), testBackup );

        assertDoesNotThrow( () ->
                            {
                                final String fromPath = testBackup.toAbsolutePath().toString();
                                restoreDatabase( Optional.empty(), fromPath, Optional.empty(), Optional.empty(), Optional.empty() );
                            } );

        final String databaseName = testBackup.getName( testBackup.getNameCount() - 1 ).toString();
        final Path restoredDatabaseFolder = Neo4jLayout.of( config ).databaseLayout( databaseName ).databaseDirectory();

        assertThat( restoredDatabaseFolder ).isNotEmptyDirectory();
    }

    @Test
    void shouldRestoreTwoDatabaseDefinedAsAList() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        final List<Path> databaseDirs = List.of( testDirectory.directory( "db1", "testDir" ),
                                                 testDirectory.directory( "db2", "testDir" ) );
        for ( Path databaseDir : databaseDirs )
        {
            FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), databaseDir );
        }

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
                    .forEach( restoredDatabaseFolder -> assertThat( restoredDatabaseFolder ).isNotEmptyDirectory() );
    }

    @Test
    void shouldRestoreAllDatabasesThatMatchTheFilter() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        final List<Path> databaseDirs = List.of( testDirectory.directory( "db1", "testDir" ),
                                                 testDirectory.directory( "db2", "testDir" ),
                                                 testDirectory.directory( "mongo", "testDir" ) );
        for ( Path databaseDir : databaseDirs )
        {
            FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), databaseDir );
        }

        var fromPath = databaseDirs.get( 0 ).getParent().toAbsolutePath().toString(); // point to testDir folder

        //when
        assertDoesNotThrow(
                () -> restoreDatabase( Optional.empty(), concatenateSubPath( fromPath, "d*" ), Optional.empty(), Optional.empty(), Optional.empty() ) );

        //then db1 and db2 are created
        databaseDirs.subList( 0, 2 ).stream()
                    .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
                    .map( path -> Neo4jLayout.of( config ).databaseLayout( path ).databaseDirectory() )
                    .forEach( restoredDatabaseFolder -> assertThat( restoredDatabaseFolder ).isNotEmptyDirectory() );

        //then mongo is not created because doesn't match the pattern
        List.of( databaseDirs.get( 2 ) ).stream()
            .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
            .map( path -> Neo4jLayout.of( config ).databaseLayout( path ).databaseDirectory() )
            .forEach( notRestoredDB -> assertThat( notRestoredDB ).doesNotExist() );
    }

    @Test
    void shouldRestoreDatabaseInCustomDatabaseDirectory() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );

        final List<Path> databaseDirs = List.of( testDirectory.directory( "db1", "testDir" ) );
        for ( Path databaseDir : databaseDirs )
        {
            FileUtils.copyDirectory( databaseAPI.databaseLayout().databaseDirectory(), databaseDir );
        }

        final var databaseRootFolder = testDirectory.directory( "databases", "restoreResult" );

        //when
        assertDoesNotThrow( () ->
                            {
                                final String fromPath = databaseDirs.stream()
                                                                    .map( dir -> dir.toAbsolutePath().toString() )
                                                                    .collect( Collectors.joining( "," ) );
                                restoreDatabase( Optional.empty(), fromPath, Optional.empty(), Optional.of( databaseRootFolder ), Optional.empty() );
                            } );

        //then db1
        databaseDirs.stream()
                    .map( db -> db.getName( db.getNameCount() - 1 ).toString() )
                    .map( path ->
                          {
                              final var newConfig = Config.newBuilder().fromConfig( config );
                              newConfig.set( GraphDatabaseInternalSettings.databases_root_path, databaseRootFolder );
                              return Neo4jLayout.of( newConfig.build() ).databaseLayout( path ).databaseDirectory();
                          } )
                    .forEach( restoredDatabaseFolder -> assertThat( restoredDatabaseFolder ).isNotEmptyDirectory() );
    }

    @Test
    void throwExceptionWhenFromContainsListOfValuesAndDatabaseParameterIsDefined()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String first = Path.of( "root" ).toString();
                                                             final String second = Path.of( "system" ).toString();

                                                             restoreDatabase( Optional.of( "test" ), first + "," + second,
                                                                              Optional.empty(), Optional.empty(), Optional.empty() );
                                                         } );

        assertThat( exception.getMessage() ).contains( "--database parameter can be applied only when --from match single path" );
    }

    @Test
    void throwExceptionWhenFromPathPointToTheRootOfTheFileSystem()
    {
        Iterator<Path> rootDirectories = FileSystems.getDefault().getRootDirectories().iterator();
        if ( !rootDirectories.hasNext() )
        {
            return;
        }
        ParameterException exception = assertThrows( ParameterException.class,
                                                     () ->
                                                     {

                                                         final String fromPath = rootDirectories.next().toString();
                                                         restoreDatabase( Optional.empty(), fromPath,
                                                                          Optional.empty(), Optional.empty(), Optional.empty() );
                                                     } );
        assertThat( exception.getCause().getMessage() ).contains( "should not point to the root of the file system" );
    }

    @Test
    void throwExceptionWhenFromContainsListOfValuesAndDatabaseRootIsNotEmpty()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String first = Path.of( "a", "b" ).toAbsolutePath().toString();
                                                             final String second = Path.of( "c", "d" ).toAbsolutePath().toString();

                                                             restoreDatabase( Optional.empty(), first + "," + second,
                                                                              Optional.empty(), Optional.of( Path.of( "k" ) ), Optional.empty() );
                                                         } );

        assertThat( exception.getMessage() ).contains( "--to-data-directory parameter can be applied only when --from match single path" );
    }

    @Test
    void throwExceptionWhenFromContainsListOfValuesAndTransactionRootIsNotEmpty()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class,
                                                         () ->
                                                         {
                                                             final String first = Path.of( "a", "b" ).toAbsolutePath().toString();
                                                             final String second = Path.of( "c", "d" ).toAbsolutePath().toString();

                                                             restoreDatabase( Optional.empty(), first + "," + second,
                                                                              Optional.empty(), Optional.empty(), Optional.of( Path.of( "k" ) ) );
                                                         } );

        assertThat( exception.getMessage() ).contains( "--to-data-tx-directory parameter can be applied only when --from match single path" );
    }

    private void restoreDatabase( Optional<String> database, String fromPath, Optional<Boolean> move,
                                  Optional<Path> databaseRootFolder, Optional<Path> transactionRootFolder )
    {
        var command = new RestoreDatabaseCli( getExtensionContext() );

        String[] args = buildArgs( database, fromPath, move, databaseRootFolder, transactionRootFolder );
        populateCommand( command, args );

        command.execute();
    }

    private String[] buildArgs( Optional<String> database,
                                String fromPath,
                                Optional<Boolean> move,
                                Optional<Path> databaseRootFolder,
                                Optional<Path> transactionsRootFolder )
    {
        List<String> args = new ArrayList<>();
        database.ifPresent( v -> args.add( "--database=" + v ) );
        args.add( "--from=" + fromPath );
        databaseRootFolder.ifPresent( v -> args.add( "--to-data-directory=" + v ) );
        transactionsRootFolder.ifPresent( v -> args.add( "--to-data-tx-directory=" + v ) );
        move.ifPresent( v -> args.add( "--move" ) );

        args.add( "--force" );

        return args.toArray( String[]::new );
    }

    private String concatenateSubPath( String... paths )
    {
        return String.join( File.separator, paths );
    }
}
