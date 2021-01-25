/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BootstrapConfigurationTest
{
    private final Config config = Config.defaults();

    @Test
    void shouldChooseEpollIfAvailable()
    {
        assumeTrue( Epoll.isAvailable() );

        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( config );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( config );

        assertEquals( EpollSocketChannel.class, cConfig.channelClass() );
        assertEquals( EpollServerSocketChannel.class, sConfig.channelClass() );
    }

    @Test
    @org.junit.jupiter.api.Disabled( "causing issues in tests" )
    void shouldChooseKqueueIfAvailable()
    {
        assumeTrue( KQueue.isAvailable() );

        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( config );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( config );

        assertEquals( KQueueSocketChannel.class, cConfig.channelClass() );
        assertEquals( KQueueServerSocketChannel.class, sConfig.channelClass() );
    }

    @Test
    void shouldChooseNioIfNoNativeAvailable()
    {
        assumeFalse( KQueue.isAvailable() || Epoll.isAvailable() );

        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( config );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( config );

        assertEquals( NioSocketChannel.class, cConfig.channelClass() );
        assertEquals( NioServerSocketChannel.class, sConfig.channelClass() );
    }

    @Test
    void shouldChooseNioIfNativeIsNotPrefered()
    {
        Config config = Config.defaults( CausalClusteringInternalSettings.use_native_transport, false );
        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( config );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( config );

        assertEquals( NioSocketChannel.class, cConfig.channelClass() );
        assertEquals( NioServerSocketChannel.class, sConfig.channelClass() );
    }
}
