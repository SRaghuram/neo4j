/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.Channel;

public class FlowControl
{
    private final Channel channel;

    public FlowControl( Channel channel )
    {
        this.channel = channel;
    }

    public void stopReading()
    {
        channel.config().setAutoRead( false );
    }

    public void continueReading()
    {
        channel.config().setAutoRead( true );
    }
}
