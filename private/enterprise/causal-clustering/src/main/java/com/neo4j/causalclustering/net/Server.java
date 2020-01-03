/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.util.concurrent.Future;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class Server extends LifecycleAdapter
{
    private final Log debugLog;
    private final Log userLog;
    private final String serverName;

    private final Executor executor;
    private final BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration;
    private final ChildInitializer childInitializer;
    private final ChannelInboundHandler parentHandler;
    private final ConnectorPortRegister portRegister;

    private EventLoopGroup workerGroup;
    private Channel channel;
    private SocketAddress listenAddress;

    public Server( ChildInitializer childInitializer, ChannelInboundHandler parentHandler, LogProvider debugLogProvider, LogProvider userLogProvider,
            SocketAddress listenAddress, String serverName, Executor executor, ConnectorPortRegister portRegister,
            BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration )
    {
        this.childInitializer = childInitializer;
        this.parentHandler = parentHandler;
        this.listenAddress = listenAddress;
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.serverName = serverName;
        this.executor = executor;
        this.portRegister = portRegister;
        this.bootstrapConfiguration = bootstrapConfiguration;
    }

    @Override
    public void start()
    {
        if ( channel != null )
        {
            return;
        }

        workerGroup = bootstrapConfiguration.eventLoopGroup( executor );

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( workerGroup )
                .channel( bootstrapConfiguration.channelClass() )
                .option( ChannelOption.SO_REUSEADDR, Boolean.TRUE )
                .localAddress( listenAddress.socketAddress() )
                .childHandler( childInitializer.asChannelInitializer() );

        if ( parentHandler != null )
        {
            bootstrap.handler( parentHandler );
        }

        try
        {
            channel = bootstrap.bind().syncUninterruptibly().channel();
            listenAddress = actualListenAddress( channel );
            registerListenAddress();
            debugLog.info( "%s: bound to '%s' with transport '%s'", serverName, listenAddress, bootstrapConfiguration.channelClass().getSimpleName() );
        }
        catch ( Exception e )
        {
            String message =
                    format( "%s: cannot bind to '%s' with transport '%s'.", serverName, listenAddress, bootstrapConfiguration.channelClass().getSimpleName() );
            userLog.error( message + " Message: " + e.getMessage() );
            debugLog.error( message, e );
            workerGroup.shutdownGracefully();
            throw e;
        }
    }

    @Override
    public void stop()
    {
        if ( channel == null )
        {
            return;
        }

        debugLog.info( serverName + ": stopping and unbinding from: " + listenAddress );
        try
        {
            deregisterListenAddress();
            channel.close().sync();
            channel = null;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            debugLog.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup != null )
        {
            // A quiet period of exactly zero cannot be used because that won't finish all queued tasks,
            // which is the guarantee we want, because we don't care about a quiet period per se.
            Future<?> fShutdown = workerGroup.shutdownGracefully( 100, 5000, TimeUnit.MILLISECONDS );
            if ( !fShutdown.awaitUninterruptibly( 15000, TimeUnit.MILLISECONDS ) )
            {
                // This is not really expected to ever happen.
                debugLog.warn( "Worker group not shutdown within time limit." );
            }
        }
        workerGroup = null;
    }

    public String name()
    {
        return serverName;
    }

    public SocketAddress address()
    {
        return listenAddress;
    }

    @Override
    public String toString()
    {
        return format( "Server[%s]", serverName );
    }

    private SocketAddress actualListenAddress( Channel channel )
    {
        var address = channel.localAddress();
        if ( address instanceof InetSocketAddress )
        {
            InetSocketAddress inetAddress = (InetSocketAddress) address;
            return new SocketAddress( inetAddress.getHostString(), inetAddress.getPort() );
        }
        return listenAddress;
    }

    private void registerListenAddress()
    {
        portRegister.register( serverName, listenAddress );
    }

    private void deregisterListenAddress()
    {
        portRegister.deregister( serverName );
    }
}
