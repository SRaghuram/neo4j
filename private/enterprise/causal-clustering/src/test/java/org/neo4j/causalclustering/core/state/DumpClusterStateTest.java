/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.DatabaseName;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.CLUSTER_ID;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.DB_NAME;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.ID_ALLOCATION;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.LAST_FLUSHED;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.LOCK_TOKEN;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_MEMBERSHIP;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.SESSION_TRACKER;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_TERM;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_VOTE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class DumpClusterStateTest
{
    @Rule
    public EphemeralFileSystemRule fsa = new EphemeralFileSystemRule();
    private File dataDir = new File( "data" );
    private ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, false );
    private String defaultDbName = DEFAULT_DATABASE_NAME;
    private CoreStateStorageService storageService;
    private LifeSupport lifeSupport = new LifeSupport();

    @Before
    public void setup()
    {
        clusterStateDirectory.initialize( fsa.get(), defaultDbName );
        storageService = new CoreStateStorageService( fsa, clusterStateDirectory, lifeSupport, NullLogProvider.getInstance(), Config.defaults() );
    }

    @Test
    public void shouldDumpClusterState() throws Exception
    {
        // given
        int numClusterStateItems = 10;
        MemberId nonDefaultMember = new MemberId( UUID.randomUUID() );
        DatabaseName nonDefaultClusterName = new DatabaseName( "foo" );
        TermState nonDefaultTermState = new TermState();
        nonDefaultTermState.update( 1L );
        ClusterId nonDefaultClusterId = new ClusterId( UUID.randomUUID() );
        createStates( nonDefaultMember, nonDefaultClusterId, nonDefaultClusterName, nonDefaultTermState );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DumpClusterState dumpTool = new DumpClusterState( fsa.get(), dataDir, new PrintStream( out ), defaultDbName, defaultDbName );

        // when
        dumpTool.dump();

        // then
        String outStr = out.toString();
        assertThat( outStr, allOf(
                containsString( nonDefaultMember.toString() ),
                containsString( nonDefaultClusterId.toString() ),
                containsString( nonDefaultClusterName.toString() ),
                containsString( nonDefaultTermState.toString() ) ) );
        int lineCount = outStr.split( System.lineSeparator() ).length;
        assertEquals( numClusterStateItems, lineCount );
    }

    private void createStates( MemberId nonDefaultMember, ClusterId nonDefaultClusterId,
            DatabaseName nonDefaultClusterName, TermState nonDefaultTermState ) throws IOException
    {

        // We're writing to 4 pieces of cluster state
        SimpleStorage<MemberId> memberIdStorage = storageService.simpleStorage( CORE_MEMBER_ID );
        SimpleStorage<DatabaseName> clusterNameStorage = storageService.simpleStorage( DB_NAME );
        SimpleStorage<ClusterId> clusterIdStorage = storageService.simpleStorage( CLUSTER_ID );
        StateStorage<TermState> termStateStateStorage = storageService.durableStorage( RAFT_TERM );

        // But still need to create all the other state, otherwise the read only DumpClusterState tool will throw
        storageService.stateStorage( LOCK_TOKEN, defaultDbName );
        storageService.stateStorage( ID_ALLOCATION, defaultDbName );
        storageService.stateStorage( SESSION_TRACKER );
        storageService.stateStorage( LAST_FLUSHED );
        storageService.stateStorage( RAFT_MEMBERSHIP );
        storageService.stateStorage( RAFT_VOTE );

        lifeSupport.start();

        memberIdStorage.writeState( nonDefaultMember );
        clusterNameStorage.writeState( nonDefaultClusterName );
        termStateStateStorage.persistStoreData( nonDefaultTermState );
        clusterIdStorage.writeState( nonDefaultClusterId );

        lifeSupport.shutdown();
    }

}
