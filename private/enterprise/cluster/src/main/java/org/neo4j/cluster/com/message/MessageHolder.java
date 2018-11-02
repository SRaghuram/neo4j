/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com.message;

/**
 * This is used to store messages generated from a StateMachine in response to an incoming message.
 * Messages stored here from a State message handling method are guaranteed to not be processed while the method
 * is running.
 */
public interface MessageHolder
{
    void offer( Message<? extends MessageType> message );
}
