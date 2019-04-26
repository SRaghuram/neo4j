/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.state.CoreDatabaseLife;
import com.neo4j.causalclustering.identity.RaftId;

/**
 * A {@link Inbound.MessageHandler} that can be started and stopped in {@link CoreDatabaseLife}.
 * It is required that if this MessageHandler delegates to another MessageHandler to handle messages
 * then the delegate will also have lifecycle methods called
 */
public interface LifecycleMessageHandler<M extends Message> extends Inbound.MessageHandler<M>
{
    void start( RaftId raftId ) throws Exception;

    void stop() throws Exception;
}
