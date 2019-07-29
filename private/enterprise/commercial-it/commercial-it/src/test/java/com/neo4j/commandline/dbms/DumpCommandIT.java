/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.dbms.archive.Dumper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DumpCommandIT extends AbstractCommandIT
{
    @Test
    void failToDumpRunningDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        Path dumpDestination = testDirectory.file( "dump1" ).toPath();
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> dumpDatabase( databaseName, dumpDestination ) );
        assertThat( exception.getMessage(), startsWith( "The database is in use. Stop database" ) );
    }

    @Test
    void failToDumpNonExistentDatabase()
    {
        Path dumpDestination = testDirectory.file( "dump2" ).toPath();
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> dumpDatabase( "foo", dumpDestination ) );
        assertThat( exception.getMessage(), startsWith( "Database does not exist: foo" ) );
    }

    @Test
    void dumpStoppedDatabase()
    {
        String databaseName = databaseAPI.databaseName();
        Path dumpDestination = testDirectory.file( "dump2" ).toPath();

        managementService.shutdownDatabase( databaseName );

        assertDoesNotThrow( () -> dumpDatabase( databaseName, dumpDestination ) );
        assertThat( dumpDestination.toFile().length(), greaterThan( 0L ) );
    }

    private void dumpDatabase( String database, Path to )
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new DumpCommand( context, new Dumper( context.err() ) );

        String[] args = {"--database=" + database, "--to=" + to.toAbsolutePath()};
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
