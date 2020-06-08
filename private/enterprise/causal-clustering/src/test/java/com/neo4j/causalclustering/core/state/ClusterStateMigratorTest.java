/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_ID;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class ClusterStateMigratorTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private LogProvider logProvider;
    private ClusterStateLayout clusterStateLayout;
    private SimpleStorage<ClusterStateVersion> clusterStateVersionStorage;
    private ClusterStateMigrator migrator;

    @BeforeEach
    void beforeEach() throws Exception
    {
        logProvider = FormattedLogProvider.toOutputStream( System.out );

        clusterStateLayout = ClusterStateLayout.of( testDirectory.directory( "data" ) );
        writeRandomClusterId( clusterStateLayout.raftIdStateFile( DEFAULT_DATABASE_NAME ) );

        clusterStateVersionStorage = new SimpleFileStorage<>( fs, clusterStateLayout.clusterStateVersionFile(), VERSION.marshal(), logProvider, INSTANCE );
        migrator = new ClusterStateMigrator( fs, clusterStateLayout, clusterStateVersionStorage, logProvider );
    }

    @Test
    void shouldDeleteClusterStateDirWhenVersionStorageDoesNotExist() throws Exception
    {
        runMigration();

        assertMigrationHappened();
    }

    @Test
    void shouldThrowWhenVersionStorageExistsButIsUnreadable() throws Exception
    {
        // create an empty file so that reading a version from it fails
        fs.mkdirs( clusterStateLayout.clusterStateVersionFile().getParentFile() );
        fs.write( clusterStateLayout.clusterStateVersionFile() ).close();

        assertThrows( UncheckedIOException.class, this::runMigration );

        assertMigrationDidNotHappen();
    }

    @Test
    void shouldNotMigrateWhenVersionStorageExistsAndHasExpectedVersion() throws Exception
    {
        clusterStateVersionStorage.writeState( new ClusterStateVersion( 1, 0 ) );

        runMigration();

        assertMigrationDidNotHappen();
    }

    @Test
    void shouldThrowWhenVersionStorageExistsButContainsUnknownVersion() throws Exception
    {
        clusterStateVersionStorage.writeState( new ClusterStateVersion( 42, 3 ) );

        assertThrows( IllegalStateException.class, this::runMigration );

        assertMigrationDidNotHappen();
    }

    @Test
    void shouldKeepMemberIdStorage() throws Exception
    {
        var memberId = new MemberId( UUID.randomUUID() );
        var memberIdFile = clusterStateLayout.memberIdStateFile();
        assertFalse( fs.fileExists( memberIdFile ) );
        var memberIdStorage = new SimpleFileStorage<>( fs, memberIdFile, CORE_MEMBER_ID.marshal(), logProvider, INSTANCE );
        memberIdStorage.writeState( memberId );
        assertTrue( fs.fileExists( memberIdFile ) );

        runMigration();

        assertMigrationHappened();
        assertEquals( memberId, memberIdStorage.readState() );
    }

    private void runMigration()
    {
        migrator.init();
    }

    private void writeRandomClusterId( File file ) throws IOException
    {
        assertFalse( fs.fileExists( file ) );
        var clusterIdStorage = new SimpleFileStorage<>( fs, file, RAFT_ID.marshal(), logProvider, INSTANCE );
        clusterIdStorage.writeState( RaftIdFactory.random() );
        assertTrue( fs.fileExists( file ) );
    }

    private void assertMigrationHappened() throws Exception
    {
        assertTrue( fs.isDirectory( clusterStateLayout.getClusterStateDirectory() ) );
        assertTrue( fs.fileExists( clusterStateLayout.clusterStateVersionFile() ) );
        assertFalse( fs.fileExists( clusterStateLayout.raftIdStateFile( DEFAULT_DATABASE_NAME ) ) );
        assertEquals( new ClusterStateVersion( 1, 0 ), clusterStateVersionStorage.readState() );
    }

    private void assertMigrationDidNotHappen()
    {
        assertTrue( fs.isDirectory( clusterStateLayout.getClusterStateDirectory() ) );
        assertTrue( fs.fileExists( clusterStateLayout.raftIdStateFile( DEFAULT_DATABASE_NAME ) ) );
    }
}
