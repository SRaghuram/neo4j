/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.channel.Channel;

import java.net.InetSocketAddress;

public class Connection
{
    private final Channel channel;
    private final InetSocketAddress socketAddress;

    public Connection( InetSocketAddress socketAddress, Channel channel )
    {
        this.socketAddress = socketAddress;
        this.channel = channel;
    }

    public Channel getChannel()
    {
        return channel;
    }

    public InetSocketAddress getSocketAddress()
    {
        return socketAddress;
    }
}
