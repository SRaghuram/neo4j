/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.ChannelPoolService;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import io.netty.channel.Channel;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class RaftChannelPoolService extends ChannelPoolService
{
    public static RaftChannelPoolService raftChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler scheduler,
            LogProvider logProvider, HandshakeClientInitializer handshakeClientInitializer )
    {
        return new RaftChannelPoolService( bootstrapConfiguration, scheduler, logProvider.getLog( RaftChannelPoolService.class ), handshakeClientInitializer );
    }

    private RaftChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler scheduler, Log log,
            HandshakeClientInitializer handshakeClientInitializer )
    {
        super( bootstrapConfiguration, scheduler, new AbstractChannelPoolHandler()
        {
            @Override
            public void channelCreated( Channel ch )
            {
                log.info( "Channel created [%s]", ch );
                ch.pipeline().addLast( handshakeClientInitializer );
            }
        } );
    }
}
