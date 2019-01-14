/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com.message;

import java.util.List;

/**
 * Poorly conceived interface to allow a MessageProcessor to be a network sender which handles batches of messages.
 * This should be replaced at some point
 */
public interface MessageSender extends MessageProcessor
{
    void process( List<Message<? extends MessageType>> message );
}
