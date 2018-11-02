/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

/**
 * Atomic broadcast API. This is implemented by {@link org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState}.
 */
public interface AtomicBroadcast
{
    void broadcast( Payload payload );
    void addAtomicBroadcastListener( AtomicBroadcastListener listener );
    void removeAtomicBroadcastListener( AtomicBroadcastListener listener );
}
