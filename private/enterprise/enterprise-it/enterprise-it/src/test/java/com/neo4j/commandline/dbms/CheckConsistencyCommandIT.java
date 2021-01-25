/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.consistency.CheckConsistencyCommand;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckConsistencyCommandIT extends AbstractCommandIT
{
    @Test
    void failToCheckConsistentOfRunningDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> checkConsistency( databaseName ) );
        assertThat( exception.getMessage() ).startsWith( "The database is in use. Stop database" );
    }

    @Test
    void failOnInvalidDatabaseName()
    {
        var exception = assertThrows( Exception.class, () -> checkConsistency( "оригинальноеназвание" ) );
        assertThat( exception ).hasMessageContaining( "Invalid database name 'оригинальноеназвание'." );
    }

    @Test
    void checkConsistencyOfStoppedDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        managementService.shutdownDatabase( databaseName );
        assertDoesNotThrow( () -> checkConsistency( databaseName ) );
    }

    @Test
    void checkConsistencyOfStoppedLowerCasedDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        managementService.shutdownDatabase( databaseName );

        assertEquals( databaseName.toLowerCase(), databaseName );
        assertDoesNotThrow( () -> checkConsistency( databaseName.toUpperCase() ) );
    }

    @Test
    void failToCheckNonExistentDatabase()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> checkConsistency( "foo" ) );
        assertThat( exception.getMessage() ).startsWith( "Database does not exist: foo" );
    }

    private void checkConsistency( String database )
    {
        var command = new CheckConsistencyCommand( getExtensionContext() );

        String[] args = {"--database=" + database};
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
