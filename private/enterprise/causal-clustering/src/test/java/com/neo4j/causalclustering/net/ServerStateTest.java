/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.core.ServerNameService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerStateTest
{
    private static final String SERVER_NAME = "serverName";

    private static final LogProvider logProvider = NullLogProvider.getInstance();

    private static Bootstrap bootstrap;
    private static EventLoopGroup clientGroup;
    private ExecutorService executor;
    private Server server;
    private Channel channel;

    @BeforeAll
    static void initialSetup()
    {
        clientGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap()
                .group( clientGroup )
                .channel( NioSocketChannel.class )
                .handler( new ChannelInitializer<NioSocketChannel>()
                {
                    @Override
                    protected void initChannel( NioSocketChannel ch )
                    {

                    }
                } );
    }

    @BeforeEach
    void setUp() throws Throwable
    {
        executor = Executors.newCachedThreadPool();
        server = createServer();
        server.init();
        assertFalse( canConnect() );
    }

    @AfterEach
    void tearDown() throws Throwable
    {
        if ( server != null )
        {
            server.stop();
            server.shutdown();
        }
        if ( channel != null )
        {
            channel.close();
        }
        executor.shutdown();
    }

    @AfterAll
    static void finalTearDown() throws ExecutionException, InterruptedException
    {
        clientGroup.shutdownGracefully().get();
    }

    @Test
    void shouldStartServerNormally() throws Throwable
    {
        server.start();
        assertTrue( canConnect() );
    }

    @Test
    void shouldRegisterAddressInPortRegister() throws Throwable
    {
        String name = "TheServer";
        ConnectorPortRegister portRegister = new ConnectorPortRegister();

        Server server = createServer( name, portRegister );
        try
        {
            assertNull( portRegister.getLocalAddress( name ) );

            server.start();
            assertEquals( new HostnamePort( server.address().getHostname(), server.address().getPort() ), portRegister.getLocalAddress( name ) );

            server.stop();
            assertNull( portRegister.getLocalAddress( name ) );
        }
        finally
        {
            server.stop();
            server.shutdown();
        }
    }

    private Server createServer()
    {
        return createServer( SERVER_NAME, new ConnectorPortRegister() );
    }

    private Server createServer( String name, ConnectorPortRegister portRegister )
    {
        return new Server( channel ->
        {}, null, new ServerNameService( logProvider, logProvider, name ), new SocketAddress( "localhost", 0 ), executor, portRegister,
                BootstrapConfiguration.serverConfig( Config.defaults() ) );
    }

    private boolean canConnect() throws InterruptedException
    {
        SocketAddress socketAddress = server.address();
        ChannelFuture channelFuture = bootstrap.connect( socketAddress.getHostname(), socketAddress.getPort() );
        channel = channelFuture.channel();
        return channelFuture.await().isSuccess();
    }
}
