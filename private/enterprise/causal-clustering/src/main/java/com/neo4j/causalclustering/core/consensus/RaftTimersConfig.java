/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.configuration.CausalClusteringSettings;

import java.time.Duration;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DurationRange;

public class RaftTimersConfig
{
    /**
     * The frequency of the heartbeats is 3 times faster than detecting the loss of a member
     */
    static final int HEARTBEAT_COUNT_IN_FAILURE_DETECTION = 3;

    private final DurationRange detectionWindow;
    private final DurationRange resolutionWindow;
    private final Duration heartbeatInterval;

    RaftTimersConfig( Config config )
    {
        detectionWindow = config.get( CausalClusteringSettings.leader_failure_detection_window );
        resolutionWindow = config.get( CausalClusteringSettings.election_failure_detection_window );

        var detectionWindowMin = detectionWindow.getMin();
        var resolutionWindowMin = resolutionWindow.getMin();
        if ( detectionWindowMin.toMillis() < resolutionWindowMin.toMillis() )
        {
            throw new IllegalArgumentException( String.format( "Failure detection timeout %s should not be shorter than failure resolution interval %s",
                                                               detectionWindowMin, resolutionWindowMin ) );
        }

        heartbeatInterval = detectionWindow.getMin().dividedBy( HEARTBEAT_COUNT_IN_FAILURE_DETECTION );
    }

    long detectionWindowMinInMillis()
    {
        return detectionWindow.getMin().toMillis();
    }

    long detectionWindowMaxInMillis()
    {
        return detectionWindow.getMax().toMillis();
    }

    long detectionDeltaInMillis()
    {
        return detectionWindow.getDelta().toMillis();
    }

    long resolutionWindowMinInMillis()
    {
        return resolutionWindow.getMin().toMillis();
    }

    long resolutionWindowMaxInMillis()
    {
        return resolutionWindow.getMax().toMillis();
    }

    long heartbeatIntervalInMillis()
    {
        return heartbeatInterval.toMillis();
    }
}
