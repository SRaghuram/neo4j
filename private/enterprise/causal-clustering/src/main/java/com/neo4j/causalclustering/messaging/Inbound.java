/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

public interface Inbound<M>
{
    void registerHandler( MessageHandler<M> handler );

    interface MessageHandler<M>
    {
        void handle( M message );
    }
}
