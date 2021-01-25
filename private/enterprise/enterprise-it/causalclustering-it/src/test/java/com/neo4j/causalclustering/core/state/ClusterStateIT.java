/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.scheduler.Group;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.isRegularFile;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.scheduler.Group.CATCHUP_CLIENT;
import static org.neo4j.scheduler.Group.CATCHUP_SERVER;
import static org.neo4j.scheduler.Group.RAFT_CLIENT;
import static org.neo4j.scheduler.Group.RAFT_SERVER;

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
            Path databaseStateDir = core.clusterStateDirectory().resolve( "db" );
            Path defaultDatabaseStateDir = databaseStateDir.resolve( DEFAULT_DATABASE_NAME );

            // global server id
            Path serverId = core.neo4jLayout().serverIdFile();

            // database specific durable storage (a/b)
            Path raftMemberIdDir = defaultDatabaseStateDir.resolve( "raft-member-id-state" );
            Path raftIdStateDir = defaultDatabaseStateDir.resolve( "raft-id-state" );
            Path lastFlushedStateDir = defaultDatabaseStateDir.resolve( "last-flushed-state" );
            Path membershipStateDir = defaultDatabaseStateDir.resolve( "membership-state" );
            Path sessionTrackerStateDir = defaultDatabaseStateDir.resolve( "session-tracker-state" );
            Path termStateDir = defaultDatabaseStateDir.resolve( "term-state" );
            Path voteStateDir = defaultDatabaseStateDir.resolve( "vote-state" );
            Path leaseStateDir = defaultDatabaseStateDir.resolve( "lease-state" );

            // database specific raft log
            Path raftLogDir = defaultDatabaseStateDir.resolve( "raft-log" );

            assertTrue( isDirectory( raftMemberIdDir ) );
            assertTrue( isDirectory( raftIdStateDir ) );
            assertTrue( isDirectory( lastFlushedStateDir ) );
            assertTrue( isDirectory( membershipStateDir ) );
            assertTrue( isDirectory( sessionTrackerStateDir ) );
            assertTrue( isDirectory( termStateDir ) );
            assertTrue( isDirectory( voteStateDir ) );
            assertTrue( isDirectory( raftLogDir ) );
            assertTrue( isDirectory( leaseStateDir ) );

            assertTrue( isRegularFile( serverId ) );
            assertTrue( isRegularFile( raftMemberIdDir.resolve( "raft-member-id" ) ) );
            assertTrue( isRegularFile( raftIdStateDir.resolve( "raft-id" ) ) );

            assertTrue( isRegularFile( lastFlushedStateDir.resolve( "last-flushed.a" ) ) );
            assertTrue( isRegularFile( lastFlushedStateDir.resolve( "last-flushed.b" ) ) );

            assertTrue( isRegularFile( membershipStateDir.resolve( "membership.a" ) ) );
            assertTrue( isRegularFile( membershipStateDir.resolve( "membership.b" ) ) );

            assertTrue( isRegularFile( sessionTrackerStateDir.resolve( "session-tracker.a" ) ) );
            assertTrue( isRegularFile( sessionTrackerStateDir.resolve( "session-tracker.b" ) ) );

            assertTrue( isRegularFile( termStateDir.resolve( "term.a" ) ) );
            assertTrue( isRegularFile( termStateDir.resolve( "term.b" ) ) );

            assertTrue( isRegularFile( voteStateDir.resolve( "vote.a" ) ) );
            assertTrue( isRegularFile( voteStateDir.resolve( "vote.b" ) ) );

            assertTrue( isRegularFile( raftLogDir.resolve( "raft.log.0" ) ) );

            assertTrue( isRegularFile( leaseStateDir.resolve( "lease.a" ) ) );
            assertTrue( isRegularFile( leaseStateDir.resolve( "lease.b" ) ) );
        }
    }

    @Test
    void shouldUseFastThreadLocalThreads()
    {
        var allThreads = Thread.getAllStackTraces().keySet();

        assertThat( nettyThreadsForGroup( RAFT_CLIENT, allThreads ), not( empty() ) );
        assertThat( nettyThreadsForGroup( RAFT_SERVER, allThreads ), not( empty() ) );

        assertThat( nettyThreadsForGroup( CATCHUP_CLIENT, allThreads ), not( empty() ) );
        assertThat( nettyThreadsForGroup( CATCHUP_SERVER, allThreads ), not( empty() ) );
    }

    private static Set<Thread> nettyThreadsForGroup( Group group, Set<Thread> allThreads )
    {
        return allThreads.stream()
                .filter( FastThreadLocalThread.class::isInstance )
                .filter( thread -> thread.getName().contains( group.groupName() ) )
                .collect( toSet() );
    }
}
