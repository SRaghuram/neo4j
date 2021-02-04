/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.Channel;

public class IncomingResponseValve
{
    private final Channel channel;

    public IncomingResponseValve( Channel channel )
    {
        this.channel = channel;
    }

    public void shut()
    {
        channel.config().setAutoRead( false );
    }

    public void open()
    {
        channel.config().setAutoRead( true );
    }
}
