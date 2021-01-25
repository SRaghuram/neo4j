/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import java.util.concurrent.Future;

import org.neo4j.logging.Log;

public class SimpleNettyChannel implements Channel
{
    private final Log log;
    private final io.netty.channel.Channel channel;
    private volatile boolean disposed;

    public SimpleNettyChannel( io.netty.channel.Channel channel, Log log )
    {
        this.channel = channel;
        this.log = log;
    }

    @Override
    public boolean isDisposed()
    {
        return disposed;
    }

    @Override
    public synchronized void dispose()
    {
        log.info( "Disposing channel: " + channel );
        disposed = true;
        channel.close();
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public Future<Void> write( Object msg )
    {
        checkDisposed();
        return channel.write( msg );
    }

    @Override
    public Future<Void> writeAndFlush( Object msg )
    {
        checkDisposed();
        return channel.writeAndFlush( msg );
    }

    private void checkDisposed()
    {
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }
    }
}
