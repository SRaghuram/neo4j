/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ClusterStateIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 0 );

    @Rule
    public FileSystemRule fsRule = new DefaultFileSystemRule();

    private Cluster<?> cluster;
    private FileSystemAbstraction fs = fsRule.get();

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldPlaceClusterStateInExpectedLocation()
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            File clusterStateDirectory = core.clusterStateDirectory();

            // simple storage
            File clusterIdStateDir = new File( clusterStateDirectory, "cluster-id-state" );
            File coreMemberIdStateDir = new File( clusterStateDirectory, "core-member-id-state" );
            File databaseNameStateDir = new File( clusterStateDirectory, "db-name-state" );

            // durable storage (a/b)
            File lastFlushedStateDir = new File( clusterStateDirectory, "last-flushed-state" );
            File membershipStateDir = new File( clusterStateDirectory, "membership-state" );
            File sessionTrackerStateDir = new File( clusterStateDirectory, "session-tracker-state" );
            File termStateDir = new File( clusterStateDirectory, "term-state" );
            File voteStateDir = new File( clusterStateDirectory, "vote-state" );

            // raft log
            File raftLogDir = new File( clusterStateDirectory, "raft-log" );

            // database specific durable storage (a/b)
            File databaseStateDir = new File( clusterStateDirectory, "db" );
            File defaultDatabaseStateDir = new File( databaseStateDir, DEFAULT_DATABASE_NAME );
            File lockTokenStateDir = new File( defaultDatabaseStateDir, "lock-token-state" );
            File idAllocationStateDir = new File( defaultDatabaseStateDir, "id-allocation-state" );

            assertTrue( clusterIdStateDir.isDirectory() );
            assertTrue( coreMemberIdStateDir.isDirectory() );
            assertTrue( databaseNameStateDir.isDirectory() );
            assertTrue( lastFlushedStateDir.isDirectory() );
            assertTrue( membershipStateDir.isDirectory() );
            assertTrue( sessionTrackerStateDir.isDirectory() );
            assertTrue( termStateDir.isDirectory() );
            assertTrue( voteStateDir.isDirectory() );
            assertTrue( raftLogDir.isDirectory() );
            assertTrue( lockTokenStateDir.isDirectory() );
            assertTrue( idAllocationStateDir.isDirectory() );

            assertTrue( new File( clusterIdStateDir, "cluster-id" ).isFile() );
            assertTrue( new File( coreMemberIdStateDir, "core-member-id" ).isFile() );
            assertTrue( new File( databaseNameStateDir, "db-name" ).isFile() );

            assertTrue( new File( lastFlushedStateDir, "last-flushed.a" ).isFile() );
            assertTrue( new File( lastFlushedStateDir, "last-flushed.b" ).isFile() );

            assertTrue( new File( membershipStateDir, "membership.a" ).isFile() );
            assertTrue( new File( membershipStateDir, "membership.b" ).isFile() );

            assertTrue( new File( sessionTrackerStateDir, "session-tracker.a" ).isFile() );
            assertTrue( new File( sessionTrackerStateDir, "session-tracker.b" ).isFile() );

            assertTrue( new File( termStateDir, "term.a" ).isFile() );
            assertTrue( new File( termStateDir, "term.b" ).isFile() );

            assertTrue( new File( voteStateDir, "vote.a" ).isFile() );
            assertTrue( new File( voteStateDir, "vote.b" ).isFile() );

            assertTrue( new File( raftLogDir, "raft.log.0" ).isFile() );

            assertTrue( new File( lockTokenStateDir, "lock-token.a" ).isFile() );
            assertTrue( new File( lockTokenStateDir, "lock-token.b" ).isFile() );

            assertTrue( new File( idAllocationStateDir, "id-allocation.a" ).isFile() );
            assertTrue( new File( idAllocationStateDir, "id-allocation.b" ).isFile() );
        }
    }

    @Test
    public void shouldMigrateDatabaseStateFromOldLocation() throws IOException
    {
        /* Used to live under

             cluster-state/lock-token-state
             cluster-state/id-allocation-state

           but have now been migrated to

             cluster-state/db/<name.db>/lock-token-state
             cluster-state/db/<name.db>/id-allocation-state
        */

        CoreClusterMember core = cluster.randomCoreMember( true ).orElseThrow( IllegalStateException::new );
        core.shutdown();

        File databaseStateDir = new File( core.clusterStateDirectory(), "db" );
        File defaultDatabaseStateDir = new File( databaseStateDir, DEFAULT_DATABASE_NAME );
        File lockTokenStateDir = new File( defaultDatabaseStateDir, "lock-token-state" );
        File idAllocationStateDir = new File( defaultDatabaseStateDir, "id-allocation-state" );

        assertTrue( lockTokenStateDir.exists() );
        assertTrue( idAllocationStateDir.exists() );

        // move to old location
        fs.moveToDirectory( lockTokenStateDir, core.clusterStateDirectory() );
        fs.moveToDirectory( idAllocationStateDir, core.clusterStateDirectory() );

        assertFalse( lockTokenStateDir.exists() );
        assertFalse( idAllocationStateDir.exists() );

        core.start();

        assertTrue( lockTokenStateDir.exists() );
        assertTrue( idAllocationStateDir.exists() );
    }
}
