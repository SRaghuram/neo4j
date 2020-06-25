/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;

import static org.assertj.core.api.Assertions.assertThat;

class StartStopLeader extends RepeatOnLeader
{
    private final Log log;
    private final Cluster cluster;
    private int succeses;
    private int runs;

    StartStopLeader( Control control, Resources resources, String databaseName )
    {
        super( control, resources, databaseName );
        this.log = resources.logProvider().getLog( getClass() );
        this.cluster = resources.cluster();
    }

    @Override
    public void doWorkOnLeader( ClusterMember leader ) throws Exception
    {
        runs++;
        var leaderFailureDetectionWindow = leader.config().get( CausalClusteringSettings.leader_failure_detection_window );

        log.info( "Stopping Leader: " + leader );

        var startTime = Instant.now();
        leader.shutdown();
        var oldLeader = leader;
        while ( oldLeader.serverId() == leader.serverId() )
        {
            leader = this.cluster.awaitLeader( databaseName );
        }
        var timeTaken = Duration.between( startTime, Instant.now() );
        log.info( "New leader for database {} is server {}", databaseName, leader.serverId() );
        if ( timeTaken.toMillis() < leaderFailureDetectionWindow.getMin().toMillis() )
        {
            succeses++;
        }
        log.info( "Leader transfer completed in %s (failure timeout %s)", timeTaken, leaderFailureDetectionWindow );

        Thread.sleep( 1000 );
        log.info( "Restarting server: {}", oldLeader.serverId() );
        oldLeader.start();
        cluster.awaitAllCoresJoinedAllRaftGroups( Set.of( databaseName ), 2, TimeUnit.MINUTES );
        log.info( "Server {} rejoined the cluster successfully.", oldLeader.serverId() );
    }

    @Override
    public void validate()
    {
        assertThat( runs ).isGreaterThan( 0 );
        assertThat( succeses * 3 )
                .as( "2/3rds of leader transfers on shutdown should be successful" )
                .isGreaterThanOrEqualTo( runs * 2 );
    }
}
