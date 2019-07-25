/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.CappedLogger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.protocol.handshake.ChannelAttribute.PROTOCOL_STACK;

public class ReconnectingChannel implements Channel
{
    private final Log log;
    private final Bootstrap bootstrap;
    private final EventLoop eventLoop;
    private final SocketAddress destination;
    private final TimeoutStrategy connectionBackoffStrategy;

    private volatile io.netty.channel.Channel channel;
    private volatile ChannelFuture fChannel;

    private volatile boolean disposed;

    private TimeoutStrategy.Timeout connectionBackoff;
    private CappedLogger cappedLogger;

    ReconnectingChannel( Bootstrap bootstrap, EventLoop eventLoop, SocketAddress destination, final Log log )
    {
        this( bootstrap, eventLoop, destination, log, new ExponentialBackoffStrategy( 100, 1600, MILLISECONDS ) );
    }

    private ReconnectingChannel( Bootstrap bootstrap, EventLoop eventLoop, SocketAddress destination, final Log log,
            TimeoutStrategy connectionBackoffStrategy )
    {
        this.bootstrap = bootstrap;
        this.eventLoop = eventLoop;
        this.destination = destination;
        this.log = log;
        this.cappedLogger = new CappedLogger( log ).setTimeLimit( 20, TimeUnit.SECONDS, Clock.systemUTC() );
        this.connectionBackoffStrategy = connectionBackoffStrategy;
        this.connectionBackoff = connectionBackoffStrategy.newTimeout();
    }

    void start()
    {
        tryConnect();
    }

    private synchronized void tryConnect()
    {
        if ( disposed )
        {
            return;
        }
        else if ( fChannel != null && !fChannel.isDone() )
        {
            return;
        }

        fChannel = connect( destination.socketAddress() );
        channel = fChannel.channel();

        fChannel.addListener( ( ChannelFuture f ) ->
        {
            if ( !f.isSuccess() )
            {
                long millis = connectionBackoff.getMillis();
                cappedLogger.warn( "Failed to connect to: " + destination.socketAddress() + ". Retrying in " + millis + " ms" );
                f.channel().eventLoop().schedule( this::tryConnect, millis, MILLISECONDS );
                connectionBackoff.increment();
            }
            else
            {
                log.info( "Connected: " + f.channel() );
                f.channel().closeFuture().addListener( closed ->
                {
                    log.warn( String.format( "Lost connection to: %s (%s)", destination, channel.remoteAddress() ) );
                    connectionBackoff = connectionBackoffStrategy.newTimeout();
                    f.channel().eventLoop().schedule( this::tryConnect, 0, MILLISECONDS );
                } );
            }
        } );
    }

    protected ChannelFuture connect( InetSocketAddress to )
    {
        return bootstrap.connect( to );
    }

    @Override
    public synchronized void dispose()
    {
        disposed = true;
        channel.close();
    }

    @Override
    public boolean isDisposed()
    {
        return disposed;
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
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
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }

        if ( channel.isActive() )
        {
            ChannelPromise promise = voidPromise ? channel.voidPromise() : channel.newPromise();
            if ( flush )
            {
                return channel.writeAndFlush( msg, promise );
            }
            else
            {
                return channel.write( msg, promise );
            }
        }
        else
        {
            BiConsumer<io.netty.channel.Channel,Object> writer;
            Promise<Void> promise = voidPromise ? channel.voidPromise() : eventLoop.newPromise();
            if ( flush )
            {
                writer = ( channel, message ) -> chain( channel.writeAndFlush( msg ), promise );
            }
            else
            {
                writer = ( channel, message ) -> chain( channel.write( msg ), promise );
            }

            deferredWrite( msg, fChannel, promise, true, writer );
            return promise;
        }
    }

    /**
     * Chains a channel future to a promise. Used when the returned promise
     * was not allocated through the channel and cannot be used as the
     * first-hand promise for the I/O operation.
     */
    private static void chain( ChannelFuture when, Promise<Void> then )
    {
        when.addListener( f -> {
            if ( f.isSuccess() )
            {
                then.setSuccess( when.get() );
            }
            else
            {
                then.setFailure( when.cause() );
            }
        } );
    }

    /**
     * Will try to reconnect once before giving up on a send. The reconnection *must* happen
     * after write was scheduled. This is necessary to provide proper ordering when a message
     * is sent right after the non-blocking channel was setup and before the server is ready
     * to accept a connection. This happens frequently in tests.
     */
    private void deferredWrite( Object msg, ChannelFuture channelFuture, Promise<Void> promise, boolean firstAttempt,
            BiConsumer<io.netty.channel.Channel,Object> writer )
    {
        channelFuture.addListener( (ChannelFutureListener) f ->
        {
            if ( f.isSuccess() )
            {
                writer.accept( f.channel(), msg );
            }
            else if ( firstAttempt )
            {
                tryConnect();
                deferredWrite( msg, fChannel, promise, false, writer );
            }
            else
            {
                promise.setFailure( f.cause() );
            }
        } );
    }

    Optional<ProtocolStack> installedProtocolStack()
    {
        return Optional.ofNullable( channel.attr( PROTOCOL_STACK ).get().getNow( null ) );
    }

    @Override
    public String toString()
    {
        return "ReconnectingChannel{" + "channel=" + channel + ", disposed=" + disposed + '}';
    }
}
