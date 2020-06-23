/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.restore.RestoreDatabaseCli;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RestoreCommandIT extends AbstractCommandIT
{
    @Test
    void failToRestoreRunningDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        File testBackup = testDirectory.directory( "testbackup" );
        FileUtils.copyRecursively( databaseAPI.databaseLayout().databaseDirectory().toFile(), testBackup );
        CommandFailedException exception = assertThrows( CommandFailedException.class, () -> restoreDatabase( databaseName, testBackup.toPath() ) );
        assertThat( exception.getMessage() ).startsWith( "The database is in use. Stop database" );
    }

    @Test
    void restoreStoppedDatabase() throws IOException
    {
        String databaseName = databaseAPI.databaseName();
        File testBackup = testDirectory.directory( "testbackup2" );
        FileUtils.copyRecursively( databaseAPI.databaseLayout().databaseDirectory().toFile(), testBackup );

        managementService.shutdownDatabase( databaseName );

        assertDoesNotThrow(() -> restoreDatabase( databaseName, testBackup.toPath() ) );
    }

    private void restoreDatabase( String database, Path from ) throws IOException
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new RestoreDatabaseCli( context );

        String[] args = {"--database=" + database, "--from=" + from.toAbsolutePath(), "--force"};
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
