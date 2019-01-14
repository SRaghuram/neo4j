/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

/**
 * Atomic broadcast listener. Implement this to receive values from any AB algorithm.
 */
public interface AtomicBroadcastListener
{
    void receive( Payload value );
}
