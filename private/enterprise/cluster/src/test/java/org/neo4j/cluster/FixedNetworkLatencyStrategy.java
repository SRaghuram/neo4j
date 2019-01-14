/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages never gets lost
 */
public class FixedNetworkLatencyStrategy
    implements NetworkLatencyStrategy
{
    private long delay;

    public FixedNetworkLatencyStrategy()
    {
        this( 0 );
    }

    public FixedNetworkLatencyStrategy( long delay )
    {
        this.delay = delay;
    }

    @Override
    public long messageDelay( Message<? extends MessageType> message, String serverIdTo )
    {
        return delay;
    }
}
