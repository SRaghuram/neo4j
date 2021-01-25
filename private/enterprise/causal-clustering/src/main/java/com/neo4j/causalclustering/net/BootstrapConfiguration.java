/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.Executor;

import org.neo4j.configuration.Config;

public interface BootstrapConfiguration<TYPE extends Channel>
{
    static BootstrapConfiguration<? extends ServerSocketChannel> serverConfig( Config config )
    {
        if ( preferNative( config ) )
        {
            if ( Epoll.isAvailable() )
            {
                return EpollBootstrapConfig.epollServerConfig();
            }
            // TODO: kqueue is causing issues, disabling temporarily
//            else if ( KQueue.isAvailable() )
//            {
//                return KQueueBootstrapConfig.kQueueServerConfig();
//            }
        }
        return NioBootstrapConfig.nioServerConfig();
    }

    static boolean preferNative( Config config )
    {
        return config.get( CausalClusteringInternalSettings.use_native_transport );
    }

    static BootstrapConfiguration<? extends SocketChannel> clientConfig( Config config )
    {
        if ( preferNative( config ) )
        {
            if ( Epoll.isAvailable() )
            {
                return EpollBootstrapConfig.epollClientConfig();
            }
            // TODO: kqueue is causing issues, disabling temporarily
//            else if ( KQueue.isAvailable() )
//            {
//                return KQueueBootstrapConfig.kQueueClientConfig();
//            }
        }
        return NioBootstrapConfig.nioClientConfig();
    }

    EventLoopGroup eventLoopGroup( Executor executor );

    Class<TYPE> channelClass();
}
