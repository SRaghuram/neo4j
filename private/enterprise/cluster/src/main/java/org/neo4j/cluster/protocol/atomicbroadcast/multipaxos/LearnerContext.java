/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;

/**
 * Context for the Learner Paxos state machine.
 */
public interface LearnerContext
    extends TimeoutsContext, LoggingContext, ConfigurationContext
{
    /*
     * How many instances the coordinator will allow to be open before taking drastic action and delivering them
     */
    int LEARN_GAP_THRESHOLD = 10;

    long getLastDeliveredInstanceId();

    void setLastDeliveredInstanceId( long lastDeliveredInstanceId );

    long getLastLearnedInstanceId();

    long getLastKnownLearnedInstanceInCluster();

    void learnedInstanceId( long instanceId );

    boolean hasDeliveredAllKnownInstances();

    void leave();

    PaxosInstance getPaxosInstance( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId );

    AtomicBroadcastSerializer newSerializer();

    Iterable<org.neo4j.cluster.InstanceId> getAlive();

    void setNextInstanceId( long id );

    void notifyLearnMiss( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId );

    org.neo4j.cluster.InstanceId getLastKnownAliveUpToDateInstance();

    void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster, org.neo4j.cluster.InstanceId instanceId );
}
