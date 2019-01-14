/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;

import java.net.InetSocketAddress;
import java.util.Objects;

import org.neo4j.helpers.HostnamePort;

public class PortRangeSocketBinder
{
    private ServerBootstrap bootstrap;
    private static final String ALL_INTERFACES_ADDRESS = "0.0.0.0";

    public PortRangeSocketBinder( ServerBootstrap bootstrap )
    {
        this.bootstrap = bootstrap;
    }

    public Connection bindToFirstAvailablePortInRange( HostnamePort serverAddress ) throws ChannelException
    {
        int[] ports = serverAddress.getPorts();
        String host = serverAddress.getHost();

        Channel channel;
        InetSocketAddress socketAddress;
        ChannelException lastException = null;

        PortIterator portIterator = new PortIterator( ports );
        while ( portIterator.hasNext() )
        {
            Integer port = portIterator.next();
            if ( host == null || host.equals( ALL_INTERFACES_ADDRESS ) )
            {
                socketAddress = new InetSocketAddress( port );
            }
            else
            {
                socketAddress = new InetSocketAddress( host, port );
            }
            try
            {
                channel = bootstrap.bind( socketAddress );
                return new Connection( socketAddress, channel );
            }
            catch ( ChannelException e )
            {
                if ( lastException != null )
                {
                    e.addSuppressed( lastException );
                }
                lastException = e;
            }
        }

        throw Objects.requireNonNull( lastException );
    }
}
