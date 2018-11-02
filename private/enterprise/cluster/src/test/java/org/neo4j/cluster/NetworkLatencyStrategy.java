/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Strategy for deciding message delay, and whether a message is actually lost. Used to test failure handling scenarios.
 */
public interface NetworkLatencyStrategy
{
    long LOST = -1;

    long messageDelay( Message<? extends MessageType> message, String serverIdTo );
}
