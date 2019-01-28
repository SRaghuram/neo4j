/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.ChannelPools;
import com.neo4j.causalclustering.net.PooledChannel;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class SenderService extends LifecycleAdapter implements Outbound<AdvertisedSocketAddress,Message>
{
    private final ChannelPools channels;
    private final Log log;

    private boolean senderServiceRunning;

    public SenderService( ChannelInitializer channelInitializer, JobScheduler scheduler, LogProvider logProvider,
            BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration )
    {
        this.channels = new ChannelPools( bootstrapConfiguration, scheduler, new ChannelInitializingHandler( channelInitializer ) );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void send( AdvertisedSocketAddress to, Message message, boolean block )
    {
        PooledChannel pooledChannel;
        if ( !senderServiceRunning )
        {
            return;
        }
        synchronized ( this )
        {
            pooledChannel = loggingBlock( to, channels.acquire( to ) );
            if ( pooledChannel == null )
            {
                return;
            }
        }

        if ( block )
        {
            loggingBlock( to, pooledChannel.channel().writeAndFlush( message ) );
            loggingBlock( to, pooledChannel.release() );
        }
        else
        {
            pooledChannel.channel().writeAndFlush( message ).addListener( future -> pooledChannel.release() );
        }
    }

    private <V> V loggingBlock( AdvertisedSocketAddress to, java.util.concurrent.Future<V> future )
    {
        try
        {
            return future.get();
        }
        catch ( ExecutionException e )
        {
            log.error( "Exception while sending to: " + to, e );
        }
        catch ( InterruptedException e )
        {
            log.info( "Interrupted while sending", e );
        }
        return null;
    }

    @Override
    public synchronized void start()
    {
        channels.start();
        senderServiceRunning = true;
    }

    @Override
    public synchronized void stop()
    {
        senderServiceRunning = false;
        channels.stop();
    }

    public Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols()
    {
        return channels.installedProtocols();
    }

    private class ChannelInitializingHandler extends AbstractChannelPoolHandler
    {
        private final ChannelInitializer channelInitializer;

        ChannelInitializingHandler( ChannelInitializer channelInitializer )
        {
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
