/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import org.neo4j.causalclustering.core.state.CoreLife;
import org.neo4j.causalclustering.identity.ClusterId;

/**
 * A {@link Inbound.MessageHandler} that can be started and stopped in {@link CoreLife}.
 * It is required that if this MessageHandler delegates to another MessageHandler to handle messages
 * then the delegate will also have lifecycle methods called
 */
public interface LifecycleMessageHandler<M extends Message> extends Inbound.MessageHandler<M>
{
    void start( ClusterId clusterId ) throws Throwable;

    void stop() throws Throwable;
}
