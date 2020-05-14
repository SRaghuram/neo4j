/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;

public class LocalNetworkPlatform
{
    private static final int SOME_BULLSHIT_PORT = 46870;
    private Log log = NullLog.getInstance();
    private ExecutorService executor;
    private Server server;
    private Client client;

    private volatile boolean isStopping;

    public void start( ProtocolInstallers serverClientContext, LogProvider logProvider ) throws Throwable
    {
        executor = Executors.newCachedThreadPool();
        log = logProvider.getLog( getClass() );
        try
        {
            log.info( "BEGIN start" );
            ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverProtocolInstaller = serverClientContext.serverInstaller();
            SocketAddress listenSocketAddress = new SocketAddress( "localhost", SOME_BULLSHIT_PORT );
            log.info( "Starting server. Binding to: %s", listenSocketAddress );
            server = new Server( serverProtocolInstaller::install, null, logProvider, logProvider, listenSocketAddress, "BenchmarkServer", executor,
                                 new ConnectorPortRegister(), BootstrapConfiguration.serverConfig( Config.defaults() ) );
            server.init();
            server.start();

            client = new Client( serverClientContext.clientInstaller() );
            log.info( "Client channel connecting to: %s", listenSocketAddress );
            // connect
            client.getOrConnect();
        }
        catch ( Throwable t )
        {
            log.error( "Exception when starting", t );
            // best effort stop
            try
            {
                stop();
            }
            catch ( Exception ignore )
            {
            }
            throw t;
        }
        finally
        {
            log.info( "END start" );
        }
    }

    public Channel channel() throws InterruptedException
    {
        if ( isStopping )
        {
            throw new IllegalStateException( "Platform is stopped" );
        }
        Client client = Objects.requireNonNull( this.client, "Client cannot be null. Please ensure the platform was properly started." );
        return client.getOrConnect();
    }

    public void stop()
    {
        log.info( "BEGIN stop" );
        isStopping = true;
        try ( ErrorHandler errorHandler = new ErrorHandler( "Tear down" ) )
        {
            if ( client != null )
            {
                errorHandler.execute( () -> client.close() );
            }
            if ( server != null )
            {
                errorHandler.execute( () -> server.stop() );
                errorHandler.execute( () -> server.shutdown() );
            }
            if ( executor != null )
            {
                executor.shutdown();
            }
        }
        finally
        {
            log.info( "END stop" );
        }
    }

    class Client implements AutoCloseable
    {
        private final NioEventLoopGroup group;
        private final Bootstrap clientBootstrap;
        private Channel channel;

        Client( ProtocolInstaller<ProtocolInstaller.Orientation.Client> clientProtocolInstaller )
        {
            group = new NioEventLoopGroup();
            clientBootstrap = new Bootstrap().group( group ).channel( NioSocketChannel.class ).handler( new ChannelInitializer<NioSocketChannel>()
            {
                @Override
                protected void initChannel( NioSocketChannel ch ) throws Exception
                {
                    clientProtocolInstaller.install( ch );
                }
            } );
        }

        Channel getOrConnect() throws InterruptedException
        {
            if ( group.isShutdown() || group.isShuttingDown() )
            {
                throw new IllegalStateException( "Client has shutdown" );
            }
            if ( channel == null )
            {
                InetSocketAddress address = new SocketAddress( server.address().getHostname(), server.address().getPort() ).socketAddress();
                log.info( "Trying to connect to: " + address );
                ChannelFuture connect = clientBootstrap.connect( address ).addListener( (ChannelFutureListener) future ->
                {
                    if ( future.isSuccess() )
                    {
                        log.info( "Successfully connected" );
                    }
                    else
                    {
                        log.warn( "Failed to connect!", future.cause() );
                    }
                } );
                channel = connect.sync().channel();
            }
            return channel;
        }

        @Override
        public void close()
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Client shutdown" ) )
            {
                if ( channel != null && channel.isOpen() )
                {
                    errorHandler.execute( () -> channel.disconnect().sync() );
                }
                errorHandler.execute( () -> group.shutdownGracefully().sync() );
            }
        }
    }
}

