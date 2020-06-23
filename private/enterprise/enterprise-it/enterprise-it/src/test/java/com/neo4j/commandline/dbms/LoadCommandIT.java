/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.archive.CompressionFormat.ZSTD;
import static org.neo4j.function.Predicates.alwaysFalse;
import static org.neo4j.function.Predicates.alwaysTrue;

@ExtendWith( {SuppressOutputExtension.class} )
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
        DatabaseLayout databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.file( "dump1" ).toPath();
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                alwaysFalse() );

        load( newDatabase, dump );

        managementService.createDatabase( newDatabase );
        GraphDatabaseService database = managementService.database( newDatabase );

        try ( Transaction tx = database.beginTx() )
        {
            assertTrue( tx.findNodes( marker ).stream().anyMatch( alwaysTrue() ) );
        }
    }

    @Test
    void loadDeletedDatabase() throws IOException
    {
        Label marker = Label.label( "marker" );
        var databaseName = databaseAPI.databaseName();
        DatabaseLayout databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.file( "dump2" ).toPath();
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                alwaysFalse() );

        managementService.dropDatabase( databaseName );
        load( databaseName, dump );

        managementService.createDatabase( databaseName );
    }

    @Test
    void overwriteDatabaseWithForce() throws IOException
    {
        Label marker = Label.label( "marker" );
        var databaseName = databaseAPI.databaseName();
        DatabaseLayout databaseLayout = databaseAPI.databaseLayout();
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
        managementService.shutdownDatabase( databaseName );

        var dump = testDirectory.file( "dump3" ).toPath();
        new Dumper( System.out ).dump( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory(), dump, ZSTD,
                alwaysFalse() );

        assertThrows( CommandFailedException.class, () -> load( databaseName, dump ) );

        assertDoesNotThrow( () -> load( databaseName, dump, true ) );
    }

    private void load( String database, Path dump )
    {
        load( database, dump, false );
    }

    private void load( String database, Path dump, boolean force )
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new LoadCommand( context, new Loader( System.out ) );

        String[] args = {"--database=" + database, "--from=" + dump.toAbsolutePath()};
        if ( force )
        {
            args = ArrayUtil.concat( args, "--force" );
        }
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
