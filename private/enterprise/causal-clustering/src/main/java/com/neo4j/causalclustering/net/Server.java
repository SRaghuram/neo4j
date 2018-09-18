/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.helper.SuspendableLifeCycle;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;

public class Server extends SuspendableLifeCycle
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
    private ListenSocketAddress listenAddress;

    public Server( ChildInitializer childInitializer, LogProvider debugLogProvider, LogProvider userLogProvider, ListenSocketAddress listenAddress,
            String serverName, Executor executor, BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration )
    {
        this( childInitializer, null, debugLogProvider, userLogProvider, listenAddress, serverName, executor, bootstrapConfiguration );
    }

    public Server( ChildInitializer childInitializer, ListenSocketAddress listenAddress, String serverName, Executor executor,
            BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration )
    {
        this( childInitializer, null, NullLogProvider.getInstance(), NullLogProvider.getInstance(), listenAddress, serverName, executor,
                bootstrapConfiguration );
    }

    public Server( ChildInitializer childInitializer, ChannelInboundHandler parentHandler, LogProvider debugLogProvider, LogProvider userLogProvider,
            ListenSocketAddress listenAddress, String serverName, Executor executor,
            BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration )
    {
        this( childInitializer, parentHandler, debugLogProvider, userLogProvider, listenAddress, serverName, executor, null, bootstrapConfiguration );
    }

    public Server( ChildInitializer childInitializer, ChannelInboundHandler parentHandler, LogProvider debugLogProvider, LogProvider userLogProvider,
            ListenSocketAddress listenAddress, String serverName, Executor executor, ConnectorPortRegister portRegister,
            BootstrapConfiguration<? extends ServerSocketChannel> bootstrapConfiguration )
    {
        super( debugLogProvider.getLog( Server.class ) );
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
    protected void init0()
    {
        // do nothing
    }

    @Override
    protected void start0()
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
            throw e;
        }
    }

    @Override
    protected void stop0()
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

        if ( workerGroup != null && workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            debugLog.warn( "Worker group not shutdown within 10 seconds." );
        }
        workerGroup = null;
    }

    @Override
    protected void shutdown0()
    {
        // do nothing
    }

    public String name()
    {
        return serverName;
    }

    public ListenSocketAddress address()
    {
        return listenAddress;
    }

    @Override
    public String toString()
    {
        return format( "Server[%s]", serverName );
    }

    private ListenSocketAddress actualListenAddress( Channel channel )
    {
        SocketAddress address = channel.localAddress();
        if ( address instanceof InetSocketAddress )
        {
            InetSocketAddress inetAddress = (InetSocketAddress) address;
            return new ListenSocketAddress( inetAddress.getHostString(), inetAddress.getPort() );
        }
        return listenAddress;
    }

    private void registerListenAddress()
    {
        if ( portRegister != null )
        {
            portRegister.register( serverName, listenAddress );
        }
    }

    private void deregisterListenAddress()
    {
        if ( portRegister != null )
        {
            portRegister.deregister( serverName );
        }
    }
}
