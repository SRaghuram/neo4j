/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com.message;

/**
 * This represents a source of messages, such as {@link org.neo4j.cluster.com.NetworkReceiver}.
 * Attach message processors to be notified when a message arrives.
 */
public interface MessageSource
{
    void addMessageProcessor( MessageProcessor messageProcessor );
}
