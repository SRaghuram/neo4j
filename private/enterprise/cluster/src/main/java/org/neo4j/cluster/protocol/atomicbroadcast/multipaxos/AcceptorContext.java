/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.protocol.LoggingContext;

/**
 * Context used by AcceptorState
 */
public interface AcceptorContext
    extends LoggingContext
{
    AcceptorInstance getAcceptorInstance( InstanceId instanceId );

    void promise( AcceptorInstance instance, long ballot );

    void accept( AcceptorInstance instance, Object value );

    void leave();
}
