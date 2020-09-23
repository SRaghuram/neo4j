/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.dbms.archive.Dumper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DumpCommandIT extends AbstractCommandIT
{
    @Test
    void failToDumpRunningDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        Path dumpDestination = testDirectory.file( "dump1" );
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> dumpDatabase( databaseName, dumpDestination ) );
        assertThat( exception.getMessage() ).startsWith( "The database is in use. Stop database" );
    }

    @Test
    void failToDumpDatabaseWithInvalidName()
    {
        Path dumpDestination = testDirectory.file( "dump1" );
        var exception = assertThrows( Exception.class, () -> dumpDatabase( "_someDb_", dumpDestination ) );
        assertThat( exception ).hasMessageContaining( "Invalid database name '_someDb_'" );
    }

    @Test
    void failToDumpNonExistentDatabase()
    {
        Path dumpDestination = testDirectory.file( "dump2" );
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> dumpDatabase( "foo", dumpDestination ) );
        assertThat( exception.getMessage() ).startsWith( "Database does not exist: foo" );
    }

    @Test
    void dumpStoppedDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        Path dumpDestination = testDirectory.file( "dump2" );

        managementService.shutdownDatabase( databaseName );

        assertDoesNotThrow( () -> dumpDatabase( databaseName, dumpDestination ) );
        assertThat( Files.size( dumpDestination ) ).isGreaterThan( 0L );
    }

    @Test
    void dumpLowerCasedStoppedDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        Path dumpDestination = testDirectory.file( "dump3" );

        managementService.shutdownDatabase( databaseName );

        assertEquals( databaseName.toLowerCase(), databaseName );

        assertDoesNotThrow( () -> dumpDatabase( databaseName.toUpperCase(), dumpDestination ) );
        assertThat( Files.size( dumpDestination ) ).isGreaterThan( 0L );
    }

    private void dumpDatabase( String database, Path to )
    {
        var context = getExtensionContext();
        var command = new DumpCommand( context, new Dumper( context.err() ) );

        String[] args = {"--database=" + database, "--to=" + to.toAbsolutePath()};
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
