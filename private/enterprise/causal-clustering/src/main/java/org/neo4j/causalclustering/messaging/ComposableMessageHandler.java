/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;

@FunctionalInterface
public interface ComposableMessageHandler
        extends Function<LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>>, LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>>>
{
    @Override
    LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> apply( LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> messageHandler );
}
