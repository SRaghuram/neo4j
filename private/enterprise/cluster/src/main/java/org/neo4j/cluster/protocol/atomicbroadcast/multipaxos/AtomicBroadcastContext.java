/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;

/**
 * Context for AtomicBroadcast statemachine.
 */
public interface AtomicBroadcastContext
    extends TimeoutsContext, ConfigurationContext, LoggingContext
{
    void addAtomicBroadcastListener( AtomicBroadcastListener listener );
    void removeAtomicBroadcastListener( AtomicBroadcastListener listener );
    void receive( Payload value );
    boolean hasQuorum();
}
