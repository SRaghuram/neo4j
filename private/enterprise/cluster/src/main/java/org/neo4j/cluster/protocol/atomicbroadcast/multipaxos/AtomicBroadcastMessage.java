/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages for the AtomicBroadcast client API. These correspond to the methods in {@link org.neo4j.cluster.protocol
 * .atomicbroadcast.AtomicBroadcast}
 * as well as internal messages for implementing the AB protocol.
 */
public enum AtomicBroadcastMessage
        implements MessageType
{
    // AtomicBroadcast API messages
    broadcast, addAtomicBroadcastListener, removeAtomicBroadcastListener,

    // Protocol implementation messages
    entered, join, leave, // Group management
    broadcastResponse, broadcastTimeout, failed // Internal message created by implementation
}
