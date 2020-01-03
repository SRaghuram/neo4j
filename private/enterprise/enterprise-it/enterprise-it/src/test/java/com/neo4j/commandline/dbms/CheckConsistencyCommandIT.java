/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.consistency.CheckConsistencyCommand;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckConsistencyCommandIT extends AbstractCommandIT
{
    @Test
    void failToCheckConsistentOfRunningDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> checkConsistency( databaseName ) );
        assertThat( exception.getMessage(), startsWith( "The database is in use. Stop database" ) );
    }

    @Test
    void checkConsistencyOfStoppedDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        managementService.shutdownDatabase( databaseName );
        assertDoesNotThrow( () -> checkConsistency( databaseName ) );
    }

    @Test
    void failToCheckNonExistentDatabase()
    {
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> checkConsistency( "foo" ) );
        assertThat( exception.getMessage(), startsWith( "Database does not exist: foo" ) );
    }

    private void checkConsistency( String database )
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new CheckConsistencyCommand( context );

        String[] args = {"--database=" + database};
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
