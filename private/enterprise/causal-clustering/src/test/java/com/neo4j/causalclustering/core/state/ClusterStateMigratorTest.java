/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;
import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_ID;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.VERSION;
import static com.neo4j.configuration.CausalClusteringSettings.DEFAULT_CLUSTER_STATE_DIRECTORY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
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

    private static Stream<String> data()
    {
        return Stream.of( DEFAULT_DATA_DIR_NAME + "/" + DEFAULT_CLUSTER_STATE_DIRECTORY_NAME, "different_place" );
    }

    @BeforeEach
    void beforeEach() throws Exception
    {
        logProvider = new Log4jLogProvider( System.out );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldDeleteClusterStateDirWhenVersionStorageDoesNotExist( String clusterStateDirectory ) throws Exception
    {
        setup( clusterStateDirectory );

        runMigration();

        assertMigrationHappened();
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldThrowWhenVersionStorageExistsButIsUnreadable( String clusterStateDirectory ) throws Exception
    {
        setup( clusterStateDirectory );

        // create an empty file so that reading a version from it fails
        fs.mkdirs( clusterStateLayout.clusterStateVersionFile().getParent() );
        fs.write( clusterStateLayout.clusterStateVersionFile() ).close();

        assertThrows( UncheckedIOException.class, this::runMigration );

        assertMigrationDidNotHappen();
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldNotMigrateWhenVersionStorageExistsAndHasExpectedVersion( String clusterStateDirectory ) throws Exception
    {
        setup( clusterStateDirectory );

        clusterStateVersionStorage.writeState( new ClusterStateVersion( 1, 0 ) );

        runMigration();

        assertMigrationDidNotHappen();
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldThrowWhenVersionStorageExistsButContainsUnknownVersion( String clusterStateDirectory ) throws Exception
    {
        setup( clusterStateDirectory );

        clusterStateVersionStorage.writeState( new ClusterStateVersion( 42, 3 ) );

        assertThrows( IllegalStateException.class, this::runMigration );

        assertMigrationDidNotHappen();
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldKeepMemberIdStorage( String clusterStateDirectory ) throws Exception
    {
        setup( clusterStateDirectory );

        var memberId = IdFactory.randomMemberId();
        var memberIdFile = clusterStateLayout.memberIdStateFile();
        assertFalse( fs.fileExists( memberIdFile ) );
        var memberIdStorage = new SimpleFileStorage<>( fs, memberIdFile, CORE_MEMBER_ID.marshal(), INSTANCE );
        memberIdStorage.writeState( memberId );
        assertTrue( fs.fileExists( memberIdFile ) );

        runMigration();

        assertMigrationHappened();
        assertEquals( memberId, memberIdStorage.readState() );
    }

    private void setup( String clusterStateDirectory ) throws IOException
    {
        clusterStateLayout = ClusterStateLayout.of( testDirectory.directory( clusterStateDirectory ) );
        writeRandomClusterId( clusterStateLayout.raftIdStateFile( DEFAULT_DATABASE_NAME ) );

        clusterStateVersionStorage = new SimpleFileStorage<>( fs, clusterStateLayout.clusterStateVersionFile(), VERSION.marshal(), INSTANCE );
        migrator = new ClusterStateMigrator( fs, clusterStateLayout, clusterStateVersionStorage, logProvider );
    }

    private void runMigration()
    {
        migrator.init();
    }

    private void writeRandomClusterId( Path file ) throws IOException
    {
        assertFalse( fs.fileExists( file ) );
        var clusterIdStorage = new SimpleFileStorage<>( fs, file, RAFT_ID.marshal(), INSTANCE );
        clusterIdStorage.writeState( IdFactory.randomRaftId() );
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
