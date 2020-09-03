/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@TestDirectoryExtension
public class CoreIdentityModuleTest
{
    @Inject
    private TestDirectory testDirectory;

    private FileSystemAbstraction fs;
    private Neo4jLayout neo4jLayout;
    private MemoryTracker memoryTracker;
    private ClusterStateStorageFactory storageFactory;
    private ClusterStateLayout clusterStateLayout;

    @BeforeEach
    void setup()
    {
        fs = testDirectory.getFileSystem();
        neo4jLayout = Neo4jLayout.of( testDirectory.homePath() );
        memoryTracker = EmptyMemoryTracker.INSTANCE;

        var logProvider = nullLogProvider();
        var config = Config.defaults( data_directory, neo4jLayout.dataDirectory() );
        clusterStateLayout = ClusterStateLayout.of( config.get( CausalClusteringSettings.cluster_state_directory ) );

        storageFactory = new ClusterStateStorageFactory( fs, clusterStateLayout, logProvider, config, memoryTracker );
    }

    @Test
    void shouldCreateServerIdAndReReadIt() throws IOException
    {
        // given
        var dataDir = neo4jLayout.dataDirectory();
        assertFalse( fs.fileExists( dataDir ) );

        // when
        var identityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );

        // then
        assertTrue( fs.fileExists( dataDir ) );
        assertTrue( fs.fileExists( neo4jLayout.serverIdFile() ) );
        assertNotNull( identityModule.serverId() );

        // when
        var secondIdentityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );

        // then
        assertEquals( identityModule.serverId(), secondIdentityModule.serverId() );

        fs.deleteRecursively( dataDir );
        var thirdIdentityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );

        // then
        assertNotEquals( secondIdentityModule.serverId(), thirdIdentityModule.serverId() );
    }

    @Test
    void shouldCreateRaftMemberIdAndReReadIt() throws IOException
    {
        // given
        var dataDir = neo4jLayout.dataDirectory();
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        assertFalse( fs.fileExists( dataDir ) );
        var raftMemberId = IdFactory.randomRaftMemberId();

        var identityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );

        // when
        identityModule.createMemberId( databaseId, raftMemberId );

        // then
        assertTrue( fs.fileExists( dataDir ) );
        assertTrue( fs.fileExists( clusterStateLayout.raftMemberIdStateFile( databaseId.name() ) ) );

        // when
        var secondIdentityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );
        var loadedId = secondIdentityModule.loadMemberId( databaseId );

        // then
        assertEquals( raftMemberId, loadedId );

        // when
        fs.deleteRecursively( clusterStateLayout.getClusterStateDirectory() );

        var thirdIdentityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );
        var newRaftMemberId = IdFactory.randomRaftMemberId();
        thirdIdentityModule.createMemberId( databaseId, newRaftMemberId );

        loadedId = thirdIdentityModule.loadMemberId( databaseId );

        // then
        assertEquals( newRaftMemberId, loadedId );
    }

    @Test
    void shouldGetRaftMemberIdOnlyAfterCreation()
    {
        // given
        var dataDir = neo4jLayout.dataDirectory();
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        assertFalse( fs.fileExists( dataDir ) );

        // when
        var identityModule = new CoreIdentityModule( nullLogProvider(), fs, neo4jLayout, memoryTracker, storageFactory );

        // then
        assertThrows( IllegalStateException.class, () -> identityModule.raftMemberId( databaseId ) );
        assertThrows( IllegalStateException.class, () -> identityModule.raftMemberId( databaseId.databaseId() ) );

        // when
        var raftMemberIdByCreation = IdFactory.randomRaftMemberId();
        identityModule.createMemberId( databaseId, raftMemberIdByCreation );
        var raftMemberIdByNamedDatabaseId = identityModule.raftMemberId( databaseId );
        var raftMemberIdByDatabaseId = identityModule.raftMemberId( databaseId.databaseId() );

        // then
        assertSame( raftMemberIdByCreation, raftMemberIdByNamedDatabaseId );
        assertSame( raftMemberIdByCreation, raftMemberIdByDatabaseId );
    }
}
