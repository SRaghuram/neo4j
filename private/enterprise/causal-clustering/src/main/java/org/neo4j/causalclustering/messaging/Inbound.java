/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

public interface Inbound<M extends Message>
{
    void registerHandler( MessageHandler<M> handler );

    interface MessageHandler<M extends Message>
    {
        void handle( M message );
    }
}
