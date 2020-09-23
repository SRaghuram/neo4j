/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.io.state.StateStorage;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.CausalClusteringSettings.DEFAULT_CLUSTER_STATE_DIRECTORY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;
import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( LifeExtension.class )
class DumpClusterStateTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private LifeSupport life;

    private Path dataDir;
    private ClusterStateStorageFactory storageFactory;

    @BeforeEach
    void setup()
    {
        dataDir = testDirectory.directory( DEFAULT_DATA_DIR_NAME );
        var clusterStateLayout = ClusterStateLayout.of( dataDir.resolve( DEFAULT_CLUSTER_STATE_DIRECTORY_NAME ) );
        storageFactory = new ClusterStateStorageFactory( testDirectory.getFileSystem(), clusterStateLayout,
                NullLogProvider.getInstance(), Config.defaults(), INSTANCE );
    }

    @Test
    void shouldDumpClusterState() throws Exception
    {
        // given
        int numClusterStateItems = 8;
        MemberId nonDefaultMember = IdFactory.randomMemberId();
        TermState nonDefaultTermState = new TermState();
        nonDefaultTermState.update( 1L );
        RaftId nonDefaultRaftId = IdFactory.randomRaftId();
        createStates( nonDefaultMember, nonDefaultRaftId, nonDefaultTermState );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DumpClusterState dumpTool = new DumpClusterState( testDirectory.getFileSystem(), dataDir, new PrintStream( out ), DEFAULT_DATABASE_NAME );

        // when
        dumpTool.dump();

        // then
        String outStr = out.toString();
        assertThat( outStr, allOf(
                containsString( nonDefaultMember.toString() ),
                containsString( nonDefaultRaftId.toString() ),
                containsString( nonDefaultTermState.toString() ) ) );
        int lineCount = outStr.split( System.lineSeparator() ).length;
        assertEquals( numClusterStateItems, lineCount );
    }

    private void createStates( MemberId nonDefaultMember, RaftId nonDefaultRaftId, TermState nonDefaultTermState ) throws IOException
    {
        // We're writing to 4 pieces of cluster state
        SimpleStorage<MemberId> memberIdStorage = storageFactory.createMemberIdStorage();
        SimpleStorage<RaftId> raftIdStorage = storageFactory.createRaftIdStorage( DEFAULT_DATABASE_NAME, nullDatabaseLogProvider() );

        StateStorage<TermState> termStateStateStorage = storageFactory.createRaftTermStorage( DEFAULT_DATABASE_NAME, life, nullDatabaseLogProvider() );

        // But still need to create all the other state, otherwise the read only DumpClusterState tool will throw
        storageFactory.createLeaseStorage( DEFAULT_DATABASE_NAME, life, nullDatabaseLogProvider() );
        storageFactory.createSessionTrackerStorage( DEFAULT_DATABASE_NAME, life, nullDatabaseLogProvider() );
        storageFactory.createLastFlushedStorage( DEFAULT_DATABASE_NAME, life, nullDatabaseLogProvider() );
        storageFactory.createRaftMembershipStorage( DEFAULT_DATABASE_NAME, life, nullDatabaseLogProvider() );
        storageFactory.createRaftVoteStorage( DEFAULT_DATABASE_NAME, life, nullDatabaseLogProvider() );

        memberIdStorage.writeState( nonDefaultMember );
        termStateStateStorage.writeState( nonDefaultTermState );
        raftIdStorage.writeState( nonDefaultRaftId );
    }
}
