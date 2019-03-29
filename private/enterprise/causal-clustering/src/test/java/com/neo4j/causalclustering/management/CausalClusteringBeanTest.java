/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.management;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.ManagementSupport;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.management.CausalClustering;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class CausalClusteringBeanTest
{
    private final FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final File dataDir = new File( "dataDir" );
    private final ClusterStateLayout clusterStateLayout = ClusterStateLayout.of( dataDir );
    private final RaftMachine raftMachine = mock( RaftMachine.class );
    private CausalClustering ccBean;

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Before
    public void setUp()
    {
        Dependencies dependencies = new Dependencies();
        Database database = mock( Database.class );
        when( database.getDatabaseName() ).thenReturn( DEFAULT_DATABASE_NAME );
        when( database.getDependencyResolver() ).thenReturn( dependencies );
        when( database.getDatabaseLayout() ).thenReturn( testDirectory.databaseLayout() );
        KernelData kernelData = new KernelData( fs, mock( PageCache.class ), new File( "storeDir" ), Config.defaults() );

        dependencies.satisfyDependency( clusterStateLayout );
        dependencies.satisfyDependency( raftMachine );
        dependencies.satisfyDependency( DatabaseInfo.CORE );

        when( database.getDependencyResolver() ).thenReturn( dependencies );
        ManagementData data = new ManagementData( new CausalClusteringBean(), kernelData, database, ManagementSupport.load() );

        ccBean = (CausalClustering) new CausalClusteringBean().createMBean( data );
    }

    @Test
    public void getCurrentRoleFromRaftMachine()
    {
        when( raftMachine.currentRole() ).thenReturn( Role.LEADER, Role.FOLLOWER, Role.CANDIDATE );
        assertEquals( "LEADER", ccBean.getRole() );
        assertEquals( "FOLLOWER", ccBean.getRole() );
        assertEquals( "CANDIDATE", ccBean.getRole() );
    }

    @Test
    public void returnSumOfRaftLogDirectory() throws Exception
    {
        File raftLogDirectory = clusterStateLayout.raftLogDirectory( DEFAULT_DATABASE_NAME );
        fs.mkdirs( raftLogDirectory );

        createFileOfSize( new File( raftLogDirectory, "raftLog1" ), 5 );
        createFileOfSize( new File( raftLogDirectory, "raftLog2" ), 10 );

        assertEquals( 15L, ccBean.getRaftLogSize() );
    }

    @Test
    public void excludeRaftLogFromReplicatedStateSize() throws Exception
    {
        // Raft log
        File raftLogDirectory = clusterStateLayout.raftLogDirectory( DEFAULT_DATABASE_NAME );
        fs.mkdirs( raftLogDirectory );
        createFileOfSize( new File( raftLogDirectory, "raftLog1" ), 5 );

        // Other state
        File idAllocationDir = clusterStateLayout.idAllocationStateDirectory( DEFAULT_DATABASE_NAME );
        fs.mkdirs( idAllocationDir );
        createFileOfSize( new File( idAllocationDir, "state" ), 10 );
        File lockTokenDir = clusterStateLayout.lockTokenStateDirectory( DEFAULT_DATABASE_NAME );
        fs.mkdirs( lockTokenDir );
        createFileOfSize( new File( lockTokenDir, "state" ), 20 );

        assertEquals( 30L, ccBean.getReplicatedStateSize() );
    }

    private void createFileOfSize( File file, int size ) throws IOException
    {
        try ( StoreChannel storeChannel = fs.write( file ) )
        {
            byte[] bytes = new byte[size];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }
    }
}
