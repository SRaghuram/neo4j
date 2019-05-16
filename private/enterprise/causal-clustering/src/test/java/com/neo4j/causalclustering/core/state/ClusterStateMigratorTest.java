/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;
import com.neo4j.causalclustering.identity.ClusterId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.CLUSTER_ID;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ClusterStateMigratorTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private ClusterStateLayout clusterStateLayout;
    private SimpleStorage<ClusterStateVersion> clusterStateVersionStorage;
    private ClusterStateMigrator migrator;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var logProvider = FormattedLogProvider.toOutputStream( System.out );

        clusterStateLayout = ClusterStateLayout.of( testDirectory.directory( "data" ) );
        writeRandomClusterId( clusterStateLayout.clusterIdStateFile(), logProvider );

        clusterStateVersionStorage = new SimpleFileStorage<>( fs, clusterStateLayout.clusterStateVersionFile(), VERSION.marshal(), logProvider );
        migrator = new ClusterStateMigrator( fs, clusterStateLayout, clusterStateVersionStorage, logProvider );
    }

    @Test
    void shouldDeleteClusterStateDirWhenVersionStorageDoesNotExist() throws Exception
    {
        migrator.migrateIfNeeded();

        assertMigrationHappened();
    }

    @Test
    void shouldDeleteClusterStateDirWhenVersionStorageIsUnreadable() throws Exception
    {
        // create an empty file so that reading a version from it fails
        fs.mkdirs( clusterStateLayout.clusterStateVersionFile().getParentFile() );
        fs.write( clusterStateLayout.clusterStateVersionFile() ).close();

        migrator.migrateIfNeeded();

        assertMigrationHappened();
    }

    @Test
    void shouldNotMigrateWhenVersionStorageExistsAndHasExpectedVersion() throws Exception
    {
        clusterStateVersionStorage.writeState( new ClusterStateVersion( 1, 0 ) );

        migrator.migrateIfNeeded();

        assertMigrationDidNotHappen();
    }

    @Test
    void shouldThrowWhenVersionStorageExistsButContainsUnknownVersion() throws Exception
    {
        clusterStateVersionStorage.writeState( new ClusterStateVersion( 42, 3 ) );

        assertThrows( IllegalStateException.class, migrator::migrateIfNeeded );
    }

    private void writeRandomClusterId( File file, FormattedLogProvider logProvider ) throws IOException
    {
        assertFalse( fs.fileExists( file ) );
        var clusterIdStorage = new SimpleFileStorage<>( fs, file, CLUSTER_ID.marshal(), logProvider );
        clusterIdStorage.writeState( new ClusterId( UUID.randomUUID() ) );
        assertTrue( fs.fileExists( file ) );
    }

    private void assertMigrationHappened() throws Exception
    {
        assertTrue( fs.isDirectory( clusterStateLayout.getClusterStateDirectory() ) );
        assertTrue( fs.fileExists( clusterStateLayout.clusterStateVersionFile() ) );
        assertFalse( fs.fileExists( clusterStateLayout.clusterIdStateFile() ) );
        assertEquals( new ClusterStateVersion( 1, 0 ), clusterStateVersionStorage.readState() );
    }

    private void assertMigrationDidNotHappen()
    {
        assertTrue( fs.isDirectory( clusterStateLayout.getClusterStateDirectory() ) );
        assertTrue( fs.fileExists( clusterStateLayout.clusterIdStateFile() ) );
    }
}
