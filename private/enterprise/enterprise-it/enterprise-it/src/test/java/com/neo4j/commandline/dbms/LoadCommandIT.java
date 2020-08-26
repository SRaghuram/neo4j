/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.io.layout.Neo4jLayout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.dbms.archive.CompressionFormat.ZSTD;
import static org.neo4j.function.Predicates.alwaysFalse;
import static org.neo4j.function.Predicates.alwaysTrue;

@EnterpriseDbmsExtension
class LoadCommandIT extends AbstractCommandIT
{
    @Test
    void failToLoadWhenDatabaseIsRunning()
    {
        var databaseName = databaseAPI.databaseName();
        var destinationPath = databaseAPI.databaseLayout().getNeo4jLayout().databasesDirectory();
        var exception = assertThrows( CommandFailedException.class, () -> load( databaseName, destinationPath ) );
        assertThat( exception.getMessage() ).contains( "The database is in use. Stop database" );
    }

    @Test
    void failToLoadDatabaseWithInvalidName()
    {
        var destinationPath = databaseAPI.databaseLayout().getNeo4jLayout().databasesDirectory();
        var exception = assertThrows( Exception.class, () -> load( "__invalid__", destinationPath ) );
        assertThat( exception ).hasMessageContaining( "Invalid database name '__invalid__'" );
    }

    @Test
    void failToLoadExistingShutdownDatabase()
    {
        var databaseName = databaseAPI.databaseName();
        var destinationPath = databaseAPI.databaseLayout().getNeo4jLayout().databasesDirectory();

        managementService.shutdownDatabase( databaseName );
        var exception = assertThrows( CommandFailedException.class, () -> load( databaseName, destinationPath ) );
        assertThat( exception.getMessage() ).contains( "Database already exists" );
    }

    @Test
    void loadNewDatabase() throws IOException
    {
        Label marker = Label.label( "marker" );
        var newDatabase = "mydatabase";
        var databaseName = databaseAPI.databaseName();
        var databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.filePath( "dump1" );
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                                       alwaysFalse() );

        load( newDatabase, dump );

        managementService.createDatabase( newDatabase );
        var database = managementService.database( newDatabase );

        try ( Transaction tx = database.beginTx() )
        {
            assertTrue( tx.findNodes( marker ).stream().anyMatch( alwaysTrue() ) );
        }
    }

    @Test
    void loadShouldntFailIfHasClusterStateForDatabase() throws IOException
    {
        var marker = Label.label( "marker" );
        var newDatabase = "mydatabase2";
        var databaseName = databaseAPI.databaseName();
        var databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.filePath( "dump2" );
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                                       alwaysFalse() );

        var config = configWith( neo4jLayout );
        createRaftGroupDirectoryFor( config, databaseName );

        load( newDatabase, dump );

        managementService.createDatabase( newDatabase );
        var database = managementService.database( newDatabase );

        try ( Transaction tx = database.beginTx() )
        {
            assertTrue( tx.findNodes( marker ).stream().anyMatch( alwaysTrue() ) );
        }
    }

    @Test
    void loadDeletedDatabase() throws IOException
    {
        var marker = Label.label( "marker" );
        var databaseName = databaseAPI.databaseName();
        var databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.filePath( "dump3" );
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                                       alwaysFalse() );

        managementService.dropDatabase( databaseName );
        load( databaseName, dump );

        managementService.createDatabase( databaseName );
    }

    @Test
    void overwriteDatabaseWithForce() throws IOException
    {
        var marker = Label.label( "marker" );
        var databaseName = databaseAPI.databaseName();
        var databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.filePath( "dump4" );
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                                       alwaysFalse() );

        assertThrows( CommandFailedException.class, () -> load( databaseName, dump ) );

        assertDoesNotThrow( () -> load( databaseName, dump, true ) );
    }

    @Test
    void enterpriseLoadShouldFailIfHasClusterStateForDatabase() throws IOException
    {
        var databaseName = databaseAPI.databaseName();
        var databaseLayout = databaseAPI.databaseLayout();
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.filePath( "dump5" );
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                                       alwaysFalse() );

        var config = configWith( neo4jLayout );
        createRaftGroupDirectoryFor( config, databaseName );

        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class, () -> enterpriseLoad( databaseName, dump ) );

        assertThat( illegalArgumentException.getMessage() ).isEqualTo(
                "Database with name [" + databaseName + "] already exists locally. " +
                "Please run `DROP DATABASE " + databaseName + "` against the system database. " +
                "If the database already is dropped, then you need to unbind the local instance using `neo4j-admin unbind`. " +
                "Note that unbind requires stopping the instance, and affects all databases."
        );
    }

    private void createRaftGroupDirectoryFor( Config config, String databaseName ) throws IOException
    {
        var raftGroupDir = ClusterStateLayout.of( config.get( GraphDatabaseSettings.data_directory ) ).raftGroupDir( databaseName );
        fs.mkdirs( raftGroupDir );
    }

    private Config configWith( Neo4jLayout layout )
    {
        return Config.defaults( neo4j_home, layout.homeDirectory() );
    }

    private void enterpriseLoad( String database, Path dump )
    {
        load( database, dump, false, true );
    }

    private void load( String database, Path dump )
    {
        load( database, dump, false, false );
    }

    private void load( String database, Path dump, boolean force )
    {
        load( database, dump, force, false );
    }

    private void load( String database, Path dump, boolean force, boolean enterprise )
    {
        var context = getExtensionContext();
        var loader = new Loader( System.out );
        var command = enterprise ? new EnterpriseLoadCommand( context, loader ) : new LoadCommand( context, loader );

        String[] args = {"--database=" + database, "--from=" + dump.toAbsolutePath()};
        if ( force )
        {
            args = ArrayUtil.concat( args, "--force" );
        }
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
