/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@TestDirectoryExtension
public class MemberIdMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    private FileSystemAbstraction fs;
    private Neo4jLayout neo4jLayout;
    private ClusterStateLayout clusterStateLayout;
    private ClusterStateStorageFactory storageFactory;

    private SimpleStorage<RaftMemberId> oldMemberIdStorage;
    private SimpleFileStorage<ServerId> serverIdStorage;
    private MemberIdMigrator migrator;

    @BeforeEach
    void setup()
    {
        fs = testDirectory.getFileSystem();
        neo4jLayout = Neo4jLayout.of( testDirectory.homePath() );
        clusterStateLayout = ClusterStateLayout.of( testDirectory.homePath() );

        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        var logProvider = nullLogProvider();
        var config = Config.defaults();

        storageFactory = new ClusterStateStorageFactory( fs, clusterStateLayout, logProvider, config, memoryTracker );
        oldMemberIdStorage = storageFactory.createOldMemberIdStorage();
        serverIdStorage = new SimpleFileStorage<>( fs, neo4jLayout.serverIdFile(), ServerId.Marshal.INSTANCE, memoryTracker );

        migrator = new MemberIdMigrator( logProvider, fs, neo4jLayout, clusterStateLayout, storageFactory, memoryTracker );
    }

    @Test
    void shouldDoNothingIfMemberIdMissing()
    {
        // when
        migrator.init();

        // then
        assertFalse( serverIdStorage.exists() );
        assertFalse( oldMemberIdStorage.exists() );
    }

    @Test
    void shouldMigrateIfMemberIdPresent() throws IOException
    {
        // given
        var oldMemberId = IdFactory.randomRaftMemberId();
        oldMemberIdStorage.writeState( oldMemberId );

        // when
        migrator.init();

        // then
        assertTrue( serverIdStorage.exists() );
        var serverId = serverIdStorage.readState();
        assertEquals( oldMemberId.uuid(), serverId.uuid() );
        assertFalse( oldMemberIdStorage.exists() );
    }

    @Test
    void shouldJustRemoveMemberIdBothIdsPresentButSame() throws IOException
    {
        // given
        var oldMemberId = IdFactory.randomRaftMemberId();
        oldMemberIdStorage.writeState( oldMemberId );
        serverIdStorage.writeState( new ServerId( oldMemberId.uuid() ) );

        // when
        migrator.init();

        // then
        assertTrue( serverIdStorage.exists() );
        var serverId = serverIdStorage.readState();
        assertEquals( oldMemberId.uuid(), serverId.uuid() );
        assertFalse( oldMemberIdStorage.exists() );
    }

    @Test
    void shouldThrowIfBothIdsPresentButDifferent() throws IOException
    {
        // given
        var oldMemberId = IdFactory.randomRaftMemberId();
        oldMemberIdStorage.writeState( oldMemberId );
        var serverId = IdFactory.randomServerId();
        serverIdStorage.writeState( serverId );

        // when/then
        assertThrows( IllegalStateException.class, migrator::init );
    }

    @Test
    void shouldMigrateOldMemberIdToRaftGroups() throws IOException
    {
        // given
        var oldMemberId = IdFactory.randomRaftMemberId();
        oldMemberIdStorage.writeState( oldMemberId );

        fs.mkdirs( clusterStateLayout.raftGroupDir( "A" ) );
        fs.mkdirs( clusterStateLayout.raftGroupDir( "B" ) );
        fs.mkdirs( clusterStateLayout.raftGroupDir( "C" ) );

        // when
        migrator.init();

        // then
        assertFalse( oldMemberIdStorage.exists() );
        assertEquals( oldMemberId.uuid(), serverIdStorage.readState().uuid() );

        assertEquals( oldMemberId.uuid(), storageFactory.createRaftMemberIdStorage( "A" ).readState().uuid() );
        assertEquals( oldMemberId.uuid(), storageFactory.createRaftMemberIdStorage( "B" ).readState().uuid() );
        assertEquals( oldMemberId.uuid(), storageFactory.createRaftMemberIdStorage( "C" ).readState().uuid() );
    }

    @Test
    void shouldContinueMemberIdMigrationInProgress() throws IOException
    {
        // given
        var oldMemberId = IdFactory.randomRaftMemberId();
        oldMemberIdStorage.writeState( oldMemberId );
        var raftMemberIdA = storageFactory.createRaftMemberIdStorage( "A" );
        raftMemberIdA.writeState( oldMemberId );

        fs.mkdirs( clusterStateLayout.raftGroupDir( "B" ) );
        fs.mkdirs( clusterStateLayout.raftGroupDir( "C" ) );

        // when
        migrator.init();

        // then
        assertFalse( oldMemberIdStorage.exists() );
        assertEquals( oldMemberId.uuid(), serverIdStorage.readState().uuid() );

        assertEquals( oldMemberId.uuid(), raftMemberIdA.readState().uuid() );
        assertEquals( oldMemberId.uuid(), storageFactory.createRaftMemberIdStorage( "B" ).readState().uuid() );
        assertEquals( oldMemberId.uuid(), storageFactory.createRaftMemberIdStorage( "C" ).readState().uuid() );
    }

    @Test
    void shouldThrowWhenOldMemberIdDoesNotMatchNewMemberId() throws IOException
    {
        // given
        var oldMemberId = IdFactory.randomRaftMemberId();
        oldMemberIdStorage.writeState( oldMemberId );

        var raftMemberIdA = storageFactory.createRaftMemberIdStorage( "A" );
        raftMemberIdA.writeState( IdFactory.randomRaftMemberId() );

        // when / then
        assertThrows( IllegalStateException.class, () -> migrator.init() );
    }
}
