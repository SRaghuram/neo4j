/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.timeout;

import org.neo4j.cluster.com.message.Message;

/**
 * Strategy for determining what timeout to use for messages.
 */
public interface TimeoutStrategy
{
    /**
     * @return the timeout (in milliseconds) for the given message.
     */
    long timeoutFor( Message message );

    void timeoutTriggered( Message timeoutMessage );

    void timeoutCancelled( Message timeoutMessage );

    void tick( long now );
}
