/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

public interface TimeoutsContext
{
    void setTimeout( Object key, Message<? extends MessageType> timeoutMessage );

    Message<? extends MessageType> cancelTimeout( Object key );

    long getTimeoutFor( Message<? extends MessageType> timeoutMessage );
}
