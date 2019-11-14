/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.ChannelPoolService;
import com.neo4j.causalclustering.protocol.init.ClientChannelInitializer;
import io.netty.channel.Channel;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class RaftChannelPoolService extends ChannelPoolService
{
    public RaftChannelPoolService( BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration, JobScheduler scheduler, LogProvider logProvider,
            ClientChannelInitializer channelInitializer )
    {
        super( bootstrapConfiguration, scheduler, Group.RAFT_CLIENT,
                new PipelineInstaller( logProvider.getLog( RaftChannelPoolService.class ), channelInitializer ), OneMultiplexedChannel::new );
    }

    private static class PipelineInstaller extends AbstractChannelPoolHandler
    {
        private final Log log;
        private final ClientChannelInitializer channelInitializer;

        PipelineInstaller( Log log, ClientChannelInitializer channelInitializer )
        {
            this.log = log;
            this.channelInitializer = channelInitializer;
        }

        @Override
        public void channelCreated( Channel ch )
        {
            log.info( "Channel created [%s]", ch );
            ch.pipeline().addLast( channelInitializer );
        }
    }
}
