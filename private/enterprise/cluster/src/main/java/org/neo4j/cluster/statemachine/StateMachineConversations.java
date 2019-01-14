/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.statemachine;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.cluster.InstanceId;

/**
 * Generate id's for state machine conversations. This should be shared between all state machines in a server.
 * <p>
 * These conversation id's can be used to uniquely identify conversations between distributed state machines.
 */
public class StateMachineConversations
{
    private final AtomicLong nextConversationId = new AtomicLong();
    private final String serverId;

    public StateMachineConversations( InstanceId me )
    {
        serverId = me.toString();
    }

    public String getNextConversationId()
    {
        return serverId + "/" + nextConversationId.incrementAndGet() + "#";
    }
}
