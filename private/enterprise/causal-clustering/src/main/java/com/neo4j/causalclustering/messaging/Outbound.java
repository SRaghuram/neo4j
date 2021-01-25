/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

/**
 * A best effort service for delivery of messages to members. No guarantees are made about any of the methods
 * in terms of eventual delivery. The only non trivial promises is that no messages get duplicated and nothing gets
 * delivered to the wrong host.
 *
 * @param <MEMBER> The type of members that messages will be sent to.
 */
public interface Outbound<MEMBER, MESSAGE>
{
    /**
     * Asynchronous, best effort delivery to destination.
     *
     * @param to destination
     * @param message The message to send
     */
    default void send( MEMBER to, MESSAGE message )
    {
        send( to, message, false );
    }

    /**
     * Best effort delivery to destination.
     * <p>
     * Blocking waits at least until the I/O operation
     * completes, but it might still have failed.
     *
     * @param to destination
     * @param message the message to send
     * @param block whether to block until I/O completion
     */
    void send( MEMBER to, MESSAGE message, boolean block );
}
