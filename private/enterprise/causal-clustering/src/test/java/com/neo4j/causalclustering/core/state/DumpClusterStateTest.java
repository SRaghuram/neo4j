/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.DatabaseName;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {TestDirectoryExtension.class, LifeExtension.class} )
class DumpClusterStateTest
{
    private static final String DB_NAME = DEFAULT_DATABASE_NAME;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private LifeSupport life;

    private File dataDir;
    private CoreStateStorageFactory storageFactory;

    @BeforeEach
    void setup()
    {
        dataDir = testDirectory.directory( "data" );
        storageFactory = new CoreStateStorageFactory( testDirectory.getFileSystem(), ClusterStateLayout.of( dataDir ),
                NullLogProvider.getInstance(), Config.defaults() );
    }

    @Test
    void shouldDumpClusterState() throws Exception
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
        DumpClusterState dumpTool = new DumpClusterState( testDirectory.getFileSystem(), dataDir, new PrintStream( out ), DB_NAME );

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
        SimpleStorage<MemberId> memberIdStorage = storageFactory.createMemberIdStorage();
        SimpleStorage<DatabaseName> clusterNameStorage = storageFactory.createMultiClusteringDbNameStorage();
        SimpleStorage<ClusterId> clusterIdStorage = storageFactory.createClusterIdStorage();

        StateStorage<TermState> termStateStateStorage = storageFactory.createRaftTermStorage( DB_NAME, life );

        // But still need to create all the other state, otherwise the read only DumpClusterState tool will throw
        storageFactory.createLockTokenStorage( DB_NAME, life );
        storageFactory.createIdAllocationStorage( DB_NAME, life );
        storageFactory.createSessionTrackerStorage( DB_NAME, life );
        storageFactory.createLastFlushedStorage( DB_NAME, life );
        storageFactory.createRaftMembershipStorage( DB_NAME, life );
        storageFactory.createRaftVoteStorage( DB_NAME, life );

        memberIdStorage.writeState( nonDefaultMember );
        clusterNameStorage.writeState( nonDefaultClusterName );
        termStateStateStorage.persistStoreData( nonDefaultTermState );
        clusterIdStorage.writeState( nonDefaultClusterId );
    }
}
