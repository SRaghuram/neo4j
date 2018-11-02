/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.heartbeat;

import java.util.List;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;

/**
 * Context used by the {@link HeartbeatState} state machine.
 */
public interface HeartbeatContext
    extends TimeoutsContext, ConfigurationContext, LoggingContext
{
    void started();

    /**
     * @return True iff the node was suspected
     */
    boolean alive( InstanceId node );

    void suspect( InstanceId node );

    void suspicions( InstanceId from, Set<InstanceId> suspicions );

    Set<InstanceId> getFailed();

    Iterable<InstanceId> getAlive();

    void addHeartbeatListener( HeartbeatListener listener );

    void removeHeartbeatListener( HeartbeatListener listener );

    void serverLeftCluster( InstanceId node );

    boolean isFailedBasedOnSuspicions( InstanceId node );

    List<InstanceId> getSuspicionsOf( InstanceId server );

    Set<InstanceId> getSuspicionsFor( InstanceId uri );

    Iterable<InstanceId> getOtherInstances();

    long getLastKnownLearnedInstanceInCluster();

    long getLastLearnedInstanceId();

    /**
     * Adds an instance in the failed set. Note that the {@link #isFailedBasedOnSuspicions(InstanceId)} method does not consult this set
     * or adds to it, instead deriving failed status from suspicions.
     */
    void failed( InstanceId instanceId );
}
