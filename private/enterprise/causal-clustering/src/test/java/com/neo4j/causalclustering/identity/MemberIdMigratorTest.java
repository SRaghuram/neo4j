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
import org.neo4j.memory.MemoryTracker;
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

    private MemberIdMigrator migrator;
    private FileSystemAbstraction fs;
    private Neo4jLayout neo4jLayout;
    private MemoryTracker memoryTracker;
    private ClusterStateStorageFactory storageFactory;

    private SimpleFileStorage<ServerId> serverIdStorage;
    private SimpleStorage<MemberId> memberIdStorage;

    @BeforeEach
    void setup()
    {
        fs = testDirectory.getFileSystem();
        neo4jLayout = Neo4jLayout.of( testDirectory.homePath() );
        memoryTracker = EmptyMemoryTracker.INSTANCE;

        var logProvider = nullLogProvider();
        var clusterStateLayout = ClusterStateLayout.of( testDirectory.homeDir() );
        var config = Config.defaults();

        storageFactory = new ClusterStateStorageFactory( fs, clusterStateLayout, logProvider, config, memoryTracker );
        migrator = MemberIdMigrator.create( logProvider, fs, neo4jLayout, memoryTracker, storageFactory );

        serverIdStorage = new SimpleFileStorage<>( fs, neo4jLayout.serverIdFile().toFile(), new ServerId.Marshal(), memoryTracker );
        memberIdStorage = storageFactory.createMemberIdStorage();
    }

    @Test
    void shouldDoNothingIfMemberIdMissing()
    {
        // when
        migrator.migrate();

        // then
        assertFalse( serverIdStorage.exists() );
        assertFalse( memberIdStorage.exists() );
    }

    @Test
    void shouldMigrateIfMemberIdPresent() throws IOException
    {
        // given
        var memberId = IdFactory.randomMemberId();
        memberIdStorage.writeState( memberId );

        // when
        migrator.migrate();

        // then
        assertTrue( serverIdStorage.exists() );
        var serverId = serverIdStorage.readState();
        assertEquals( memberId.getUuid(), serverId.getUuid() );
        assertFalse( memberIdStorage.exists() );
    }

    @Test
    void shouldJustRemoveMemberIdBothIdsPresentButSame() throws IOException
    {
        // given
        var memberId = IdFactory.randomMemberId();
        memberIdStorage.writeState( memberId );
        serverIdStorage.writeState( memberId );

        // when
        migrator.migrate();

        // then
        assertTrue( serverIdStorage.exists() );
        var serverId = serverIdStorage.readState();
        assertEquals( memberId.getUuid(), serverId.getUuid() );
        assertFalse( memberIdStorage.exists() );
    }

    @Test
    void shouldThrowIfBothIdsPresentButDifferent() throws IOException
    {
        // given
        var memberId = IdFactory.randomMemberId();
        memberIdStorage.writeState( memberId );
        var serverId = IdFactory.randomServerId();
        serverIdStorage.writeState( serverId );

        // when/then
        assertThrows( IllegalStateException.class, migrator::migrate );
    }

    @Test
    void shouldRenameOldServerId() throws IOException
    {
        // given
        var oldServerIdStorage = new SimpleFileStorage<>(
                fs, neo4jLayout.dataDirectory().resolve( "server-id" ).toFile(), new ServerId.Marshal(), memoryTracker );
        var serverId = IdFactory.randomServerId();
        oldServerIdStorage.writeState( serverId );

        // when
        migrator.migrate();

        // then
        assertTrue( serverIdStorage.exists() );
        var readServerId = serverIdStorage.readState();
        assertFalse( oldServerIdStorage.exists() );
    }
}
