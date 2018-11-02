/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.statemachine;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Implemented by states in a state machine. Each state must
 * implement the handle method, to perform different things depending
 * on the message that comes in. This should only be implemented as enums.
 * <p>
 * A state is guaranteed to only have one handle at a time, i.e. access is serialized.
 */
public interface State<CONTEXT, MESSAGETYPE extends MessageType>
{
    /**
     * Handle a message. The state can use context for state storage/retrieval and it will also act
     * as a facade to the rest of the system. The MessageProcessor is used to trigger new messages.
     * When the handling is done the state returns the next state of the state machine.
     *
     *
     * @param context  that contains state and methods to access other parts of the system
     * @param message  that needs to be handled
     * @param outgoing processor for new messages created by the handling of this message
     * @return the new state
     */
    State<CONTEXT, MESSAGETYPE> handle( CONTEXT context, Message<MESSAGETYPE> message, MessageHolder outgoing ) throws Throwable;
}
