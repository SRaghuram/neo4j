/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

/**
 * Store for Acceptor Paxos instances
 */
public interface AcceptorInstanceStore
{
    AcceptorInstance getAcceptorInstance( InstanceId instanceId );

    void promise( AcceptorInstance instance, long ballot );

    void accept( AcceptorInstance instance, Object value );

    void lastDelivered( InstanceId instanceId );

    void clear();
}
