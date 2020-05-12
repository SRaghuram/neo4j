/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.Config;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.failure_detection_window;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.failure_resolution_window;
import static com.neo4j.causalclustering.core.consensus.RaftTimersConfig.HEARTBEAT_COUNT_IN_FAILURE_DETECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.helpers.DurationRange.fromSeconds;

public class RaftTimersConfigTest
{
    @Test
    void nothingSetShouldReturnDefaults()
    {
        var clusterConfig = Config.newBuilder();
        var raftTimersConfig = new RaftTimersConfig( clusterConfig.build() );

        assertEquals( raftTimersConfig.detectionWindowMinInMillis(), 20000 );
        assertEquals( raftTimersConfig.detectionWindowMaxInMillis(), 23000 );
        assertEquals( raftTimersConfig.detectionDeltaInMillis(), 3000 );
        assertEquals( raftTimersConfig.resolutionWindowMinInMillis(), 3000 );
        assertEquals( raftTimersConfig.resolutionWindowMaxInMillis(), 6000 );
        assertEquals( raftTimersConfig.heartbeatIntervalInMillis(), 20000 / HEARTBEAT_COUNT_IN_FAILURE_DETECTION );
    }

    @Test
    void oldSettingPresentShouldReturnOldSettings()
    {
        var config = Config.newBuilder()
                           .setRaw( Map.of( "causal_clustering.leader_election_timeout", "10s" ) )
                           .build();
        var raftTimersConfig = new RaftTimersConfig( config );

        assertEquals( raftTimersConfig.detectionWindowMinInMillis(), 10000 );
        assertEquals( raftTimersConfig.detectionWindowMaxInMillis(), 15000 );
        assertEquals( raftTimersConfig.detectionDeltaInMillis(), 5000 );
        assertEquals( raftTimersConfig.resolutionWindowMinInMillis(), 3000 );
        assertEquals( raftTimersConfig.resolutionWindowMaxInMillis(), 6000 );
        assertEquals( raftTimersConfig.heartbeatIntervalInMillis(), 10000 / HEARTBEAT_COUNT_IN_FAILURE_DETECTION );
    }

    @Test
    void oldSettingShorterThan5SecondsWindowShouldBeDouble()
    {
        var config = Config.newBuilder()
                           .setRaw( Map.of( "causal_clustering.leader_election_timeout", "4s" ) )
                           .build();
        var raftTimersConfig = new RaftTimersConfig( config );

        assertEquals( raftTimersConfig.detectionWindowMinInMillis(), 4000 );
        assertEquals( raftTimersConfig.detectionWindowMaxInMillis(), 8000 );
        assertEquals( raftTimersConfig.detectionDeltaInMillis(), 4000 );
        assertEquals( raftTimersConfig.resolutionWindowMinInMillis(), 3000 );
        assertEquals( raftTimersConfig.resolutionWindowMaxInMillis(), 6000 );
        assertEquals( raftTimersConfig.heartbeatIntervalInMillis(), 4000 / HEARTBEAT_COUNT_IN_FAILURE_DETECTION );
    }

    @Test
    void oldSettingShorterThanResolutionWindowShouldOverwrite()
    {
        var config = Config.newBuilder()
                           .setRaw( Map.of( "causal_clustering.leader_election_timeout", "1s" ) )
                           .build();
        var raftTimersConfig = new RaftTimersConfig( config );

        assertEquals( raftTimersConfig.detectionWindowMinInMillis(), 1000 );
        assertEquals( raftTimersConfig.detectionWindowMaxInMillis(), 2000 );
        assertEquals( raftTimersConfig.detectionDeltaInMillis(), 1000 );
        assertEquals( raftTimersConfig.resolutionWindowMinInMillis(), 1000 );
        assertEquals( raftTimersConfig.resolutionWindowMaxInMillis(), 2000 );
        assertEquals( raftTimersConfig.heartbeatIntervalInMillis(), 1000 / HEARTBEAT_COUNT_IN_FAILURE_DETECTION );
    }

    @Test
    void newSettingPresentShouldIgnoreOldSetting()
    {
        var config = Config.newBuilder()
                           .setRaw( Map.of( "causal_clustering.leader_election_timeout", "1s" ) )
                           .set( failure_detection_window, fromSeconds( 10, 12 ) )
                           .set( failure_resolution_window, fromSeconds( 8, 9 ) )
                           .build();
        var raftTimersConfig = new RaftTimersConfig( config );

        assertEquals( raftTimersConfig.detectionWindowMinInMillis(), 10000 );
        assertEquals( raftTimersConfig.detectionWindowMaxInMillis(), 12000 );
        assertEquals( raftTimersConfig.detectionDeltaInMillis(), 2000 );
        assertEquals( raftTimersConfig.resolutionWindowMinInMillis(), 8000 );
        assertEquals( raftTimersConfig.resolutionWindowMaxInMillis(), 9000 );
        assertEquals( raftTimersConfig.heartbeatIntervalInMillis(), 10000 / HEARTBEAT_COUNT_IN_FAILURE_DETECTION );
    }

    @Test
    void invalidRangeShouldThrow()
    {
        var configBuilder = Config.newBuilder();
        assertThrows( IllegalArgumentException.class, () -> configBuilder.set( failure_detection_window, fromSeconds( 12, 10 ) ) );
    }

    @Test
    void shorterDetectionThanResolutionShouldThrow()
    {
        var config = Config.newBuilder()
                           .set( failure_detection_window, fromSeconds( 5, 6 ) )
                           .set( failure_resolution_window, fromSeconds( 8, 9 ) )
                           .build();
        assertThrows( IllegalArgumentException.class, () -> new RaftTimersConfig( config ) );
    }
}
