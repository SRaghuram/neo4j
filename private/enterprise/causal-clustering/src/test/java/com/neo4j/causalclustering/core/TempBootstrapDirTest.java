/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Neo4jLayoutExtension
class TempBootstrapDirTest
{
    @Inject
    DatabaseLayout databaseLayout;

    @Inject
    FileSystemAbstraction fileSystem;

    @Test
    void shouldCleanDirectoryBeforeAndAfter() throws IOException
    {
        createTempBootstrapDir();
        assertTrue( fileSystem.fileExists( tempBootstrapDir() ) );

        try ( var dir = TempBootstrapDir.cleanBeforeAndAfter( fileSystem, databaseLayout ) )
        {
            assertFalse( fileSystem.fileExists( tempBootstrapDir() ) );

            assertEquals( tempBootstrapDir(), dir.get() );
            createTempBootstrapDir();
            assertTrue( fileSystem.fileExists( tempBootstrapDir() ) );
        }

        assertFalse( fileSystem.fileExists( tempBootstrapDir() ) );
    }

    @Test
    void shouldDeleteWhenInvoked() throws IOException
    {
        var dir = new TempBootstrapDir( fileSystem, databaseLayout );

        createTempBootstrapDir();
        assertTrue( fileSystem.fileExists( tempBootstrapDir() ) );

        dir.delete();
        assertFalse( fileSystem.fileExists( tempBootstrapDir() ) );
    }

    private void createTempBootstrapDir() throws IOException
    {
        fileSystem.mkdir( tempBootstrapDir() );
    }

    private Path tempBootstrapDir()
    {
        return databaseLayout.databaseDirectory().resolve( CausalClusteringInternalSettings.TEMP_BOOTSTRAP_DIRECTORY_NAME );
    }
}
