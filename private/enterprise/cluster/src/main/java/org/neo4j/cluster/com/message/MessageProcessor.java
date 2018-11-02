/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com.message;

/**
 * This is used to process a single message to or from a {@link org.neo4j.cluster.statemachine.StateMachine}.
 * They can be chained internally if needed, so that one processor delegates to one or more other processors.
 */
public interface MessageProcessor
{
    boolean process( Message<? extends MessageType> message );
}
