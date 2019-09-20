/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.helper.CountdownTimer;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.protocol.handshake.ChannelAttribute.PROTOCOL_STACK;

public class ReconnectingChannel implements Channel
{
    private final Log log;
    private final Bootstrap bootstrap;
    private final EventLoop eventLoop;
    private final SocketAddress destination;
    private final Duration reconnectionBackoff;

    private final CountdownTimer reconnectionTimer = new CountdownTimer();
    private boolean disposed;

    private volatile Promise<io.netty.channel.Channel> fChannel;

    ReconnectingChannel( Bootstrap bootstrap, EventLoop eventLoop, SocketAddress destination, Duration reconnectionBackoff, Log log )
    {
        this.bootstrap = bootstrap;
        this.eventLoop = eventLoop;
        this.destination = destination;
        this.reconnectionBackoff = reconnectionBackoff;
        this.log = log;
    }

    /**
     * Ensures that there either is an open connection or that a connection attempt is in progress.
     */
    private synchronized Promise<io.netty.channel.Channel> ensureConnect()
    {
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }
        else if ( fChannel != null )
        {
            if ( !fChannel.isDone() )
            {
                return fChannel;
            }

            io.netty.channel.Channel channel = fChannel.getNow();
            if ( channel != null )
            {
                if ( channel.isOpen() )
                {
                    // channel open or active, so no need to set up a new channel
                    return fChannel;
                }
                else
                {
                    channel.close(); // should not happen, but defensive against netty
                }
            }
        }

        fChannel = eventLoop.newPromise();
        Duration timeToConnect = reconnectionTimer.timeToExpiry();
        eventLoop.schedule( this::connect, timeToConnect.toMillis(), MILLISECONDS );

        return fChannel;
    }

    private void connect()
    {
        reconnectionTimer.set( reconnectionBackoff );
        bootstrap.connect( destination.socketAddress() ).addListener( (GenericFutureListener<ChannelFuture>) this::finishAttempt );
    }

    private synchronized void finishAttempt( ChannelFuture cf )
    {
        if ( disposed )
        {
            // fChannel cancelled in dispose
            cf.channel().close();
        }
        else if ( !cf.isSuccess() )
        {
            fChannel.setFailure( cf.cause() );
            log.warn( "Failed to connect to: " + destination.socketAddress() );
        }
        else
        {
            io.netty.channel.Channel channel = cf.channel();
            fChannel.setSuccess( channel );
            log.info( "Connected: " + channel );

            channel.closeFuture().addListener(
                    ignored -> log.warn( String.format( "Lost connection to: %s (%s)", destination, channel.remoteAddress() ) ) );
        }
    }

    @Override
    public synchronized void dispose()
    {
        disposed = true;

        if ( fChannel != null )
        {
            fChannel.cancel( true );

            io.netty.channel.Channel channel = fChannel.getNow();
            if ( channel != null )
            {
                channel.close();
            }
        }
    }

    @Override
    public Future<Void> write( Object msg )
    {
        return write( msg, false, false );
    }

    @Override
    public Future<Void> writeAndFlush( Object msg )
    {
        return write( msg, true, false );
    }

    @Override
    public void writeAndForget( Object msg )
    {
        write( msg, false, true );
    }

    @Override
    public void writeFlushAndForget( Object msg )
    {
        write( msg, true, true );
    }

    private Future<Void> write( Object msg, boolean flush, boolean voidPromise )
    {
        io.netty.channel.Channel activeChannel = activeChannel();

        if ( activeChannel != null )
        {
            ChannelPromise promise = voidPromise ? activeChannel.voidPromise() : activeChannel.newPromise();
            if ( flush )
            {
                return activeChannel.writeAndFlush( msg, promise );
            }
            else
            {
                return activeChannel.write( msg, promise );
            }
        }

        Promise<Void> writePromise = eventLoop.newPromise();
        ensureConnect().addListener( (GenericFutureListener<io.netty.util.concurrent.Future<io.netty.channel.Channel>>) future ->
        {
            if ( future.isSuccess() )
            {
                io.netty.channel.Channel channel = future.getNow();
                chain( flush ? channel.writeAndFlush( msg ) : channel.write( msg ), writePromise );
            }
            else
            {
                writePromise.setFailure( future.cause() );
            }
        } );
        return writePromise;
    }

    /**
     * Chains a channel future to a promise. Used when the returned promise
     * was not allocated through the channel and cannot be used as the
     * first-hand promise for the I/O operation.
     */
    private static void chain( ChannelFuture channelFuture, Promise<Void> externalPromise )
    {
        channelFuture.addListener( f ->
        {
            if ( f.isSuccess() )
            {
                externalPromise.setSuccess( channelFuture.get() );
            }
            else
            {
                externalPromise.setFailure( channelFuture.cause() );
            }
        } );
    }

    private io.netty.channel.Channel activeChannel()
    {
        if ( fChannel == null )
        {
            return null;
        }

        io.netty.channel.Channel channel = fChannel.getNow();

        if ( channel != null && channel.isActive() )
        {
            return channel;
        }

        return null;
    }

    private Optional<io.netty.channel.Channel> channel()
    {
        return ofNullable( fChannel ).map( io.netty.util.concurrent.Future::getNow );
    }

    Optional<ProtocolStack> installedProtocolStack()
    {
        return channel().map( ch -> ch.attr( PROTOCOL_STACK ).get() ).map( fProtocol -> fProtocol.getNow( null ) );
    }

    @Override
    public String toString()
    {
        return "ReconnectingChannel{" + "destination=" + destination + ", fChannel=" + fChannel + ", disposed=" + disposed + '}';
    }
}
