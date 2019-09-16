/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ClusterExtension
@ExtendWith( DefaultFileSystemExtension.class )
class ClusterStateIT
{
    @Inject
    private static ClusterFactory clusterFactory;
    @Inject
    private static FileSystemAbstraction fs;

    private static Cluster cluster;

    @BeforeAll
    static void startCluster() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldPlaceClusterStateInExpectedLocation()
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            File clusterStateDirectory = core.clusterStateDirectory();
            File databaseStateDir = new File( core.clusterStateDirectory(), "db" );
            File defaultDatabaseStateDir = new File( databaseStateDir, DEFAULT_DATABASE_NAME );

            // global simple storage
            File coreMemberIdStateDir = new File( clusterStateDirectory, "core-member-id-state" );

            // database specific durable storage (a/b)
            File raftIdStateDir = new File( defaultDatabaseStateDir, "raft-id-state" );
            File lastFlushedStateDir = new File( defaultDatabaseStateDir, "last-flushed-state" );
            File membershipStateDir = new File( defaultDatabaseStateDir, "membership-state" );
            File sessionTrackerStateDir = new File( defaultDatabaseStateDir, "session-tracker-state" );
            File termStateDir = new File( defaultDatabaseStateDir, "term-state" );
            File voteStateDir = new File( defaultDatabaseStateDir, "vote-state" );
            File barrierTokenStateDir = new File( defaultDatabaseStateDir, "barrier-token-state" );

            // database specific raft log
            File raftLogDir = new File( defaultDatabaseStateDir, "raft-log" );

            assertTrue( raftIdStateDir.isDirectory() );
            assertTrue( coreMemberIdStateDir.isDirectory() );
            assertTrue( lastFlushedStateDir.isDirectory() );
            assertTrue( membershipStateDir.isDirectory() );
            assertTrue( sessionTrackerStateDir.isDirectory() );
            assertTrue( termStateDir.isDirectory() );
            assertTrue( voteStateDir.isDirectory() );
            assertTrue( raftLogDir.isDirectory() );
            assertTrue( barrierTokenStateDir.isDirectory() );

            assertTrue( new File( raftIdStateDir, "raft-id" ).isFile() );
            assertTrue( new File( coreMemberIdStateDir, "core-member-id" ).isFile() );

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

            assertTrue( new File( barrierTokenStateDir, "barrier-token.a" ).isFile() );
            assertTrue( new File( barrierTokenStateDir, "barrier-token.b" ).isFile() );
        }
    }
}
