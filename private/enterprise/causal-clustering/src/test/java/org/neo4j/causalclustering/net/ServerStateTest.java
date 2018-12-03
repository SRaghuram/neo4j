/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.causalclustering.helper.SuspendableLifeCycleLifeStateChangeTest;
import org.neo4j.causalclustering.helper.SuspendableLifeCycleSuspendedStateChangeTest;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * More generalized state tests of SuspendableLifeCycle can be found {@link SuspendableLifeCycleLifeStateChangeTest} and
 * {@link SuspendableLifeCycleSuspendedStateChangeTest}
 */
public class ServerStateTest
{
    private static final String SERVER_NAME = "serverName";

    private static final LogProvider logProvider = NullLogProvider.getInstance();

    private static Bootstrap bootstrap;
    private static EventLoopGroup clientGroup;
    private ExecutorService executor;
    private Server server;
    private Channel channel;

    @BeforeClass
    public static void initialSetup()
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

    @Before
    public void setUp() throws Throwable
    {
        executor = Executors.newCachedThreadPool();
        server = createServer();
        server.init();
        assertFalse( canConnect() );
    }

    @After
    public void tearDown() throws Throwable
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

    @AfterClass
    public static void finalTearDown()
    {
        clientGroup.shutdownGracefully();
    }

    @Test
    public void shouldStartServerNormally() throws Throwable
    {
        server.start();
        assertTrue( canConnect() );
    }

    @Test
    public void canDisableAndEnableServer() throws Throwable
    {
        server.start();
        assertTrue( canConnect() );

        server.disable();
        assertFalse( canConnect() );

        server.enable();
        assertTrue( canConnect() );
    }

    @Test
    public void serverCannotBeEnabledIfLifeCycleHasNotStarted() throws Throwable
    {
        server.enable();
        assertFalse( canConnect() );

        server.start();
        assertTrue( canConnect() );
    }

    @Test
    public void serverCannotStartIfDisabled() throws Throwable
    {
        server.disable();

        server.start();
        assertFalse( canConnect() );

        server.enable();
        assertTrue( canConnect() );
    }

    @Test
    public void shouldRegisterAddressInPortRegister() throws Throwable
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
        return new Server( channel -> {}, null, logProvider, logProvider, new ListenSocketAddress( "localhost", 0 ),
                name, executor, portRegister );
    }

    private boolean canConnect() throws InterruptedException
    {
        ListenSocketAddress socketAddress = server.address();
        ChannelFuture channelFuture = bootstrap.connect( socketAddress.getHostname(), socketAddress.getPort() );
        channel = channelFuture.channel();
        return channelFuture.await().isSuccess();
    }
}
