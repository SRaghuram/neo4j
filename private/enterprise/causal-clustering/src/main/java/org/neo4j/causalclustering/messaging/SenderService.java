/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class SenderService extends LifecycleAdapter implements Outbound<AdvertisedSocketAddress,Message>
{
    private ReconnectingChannels channels;

    private final ChannelInitializer channelInitializer;
    private final ReadWriteLock serviceLock = new ReentrantReadWriteLock();
    private final JobScheduler scheduler;
    private final Log log;

    private boolean senderServiceRunning;
    private Bootstrap bootstrap;
    private NioEventLoopGroup eventLoopGroup;

    public SenderService( ChannelInitializer channelInitializer, JobScheduler scheduler, LogProvider logProvider )
    {
        this.channelInitializer = channelInitializer;
        this.scheduler = scheduler;
        this.log = logProvider.getLog( getClass() );
        this.channels = new ReconnectingChannels();
    }

    @Override
    public void send( AdvertisedSocketAddress to, Message message, boolean block )
    {
        Future<Void> future;
        serviceLock.readLock().lock();
        try
        {
            if ( !senderServiceRunning )
            {
                return;
            }

            future = channel( to ).writeAndFlush( message );
        }
        finally
        {
            serviceLock.readLock().unlock();
        }

        if ( block )
        {
            try
            {
                future.get();
            }
            catch ( ExecutionException e )
            {
                log.error( "Exception while sending to: " + to, e );
            }
            catch ( InterruptedException e )
            {
                log.info( "Interrupted while sending", e );
            }
        }
    }

    private Channel channel( AdvertisedSocketAddress destination )
    {
        ReconnectingChannel channel = channels.get( destination );

        if ( channel == null )
        {
            channel = new ReconnectingChannel( bootstrap, eventLoopGroup.next(), destination, log );
            channel.start();
            ReconnectingChannel existingNonBlockingChannel = channels.putIfAbsent( destination, channel );

            if ( existingNonBlockingChannel != null )
            {
                channel.dispose();
                channel = existingNonBlockingChannel;
            }
            else
            {
                log.info( "Creating channel to: [%s] ", destination );
            }
        }

        return channel;
    }

    @Override
    public synchronized void start()
    {
        serviceLock.writeLock().lock();
        try
        {
            eventLoopGroup = new NioEventLoopGroup( 0, scheduler.executor( Group.RAFT_CLIENT ) );
            bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( channelInitializer );

            senderServiceRunning = true;
        }
        finally
        {
            serviceLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void stop()
    {
        serviceLock.writeLock().lock();
        try
        {
            senderServiceRunning = false;

            Iterator<ReconnectingChannel> itr = channels.values().iterator();
            while ( itr.hasNext() )
            {
                Channel timestampedChannel = itr.next();
                timestampedChannel.dispose();
                itr.remove();
            }

            try
            {
                eventLoopGroup.shutdownGracefully( 0, 0, MICROSECONDS ).sync();
            }
            catch ( InterruptedException e )
            {
                log.warn( "Interrupted while stopping sender service." );
            }
        }
        finally
        {
            serviceLock.writeLock().unlock();
        }
    }

    public Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols()
    {
        return channels.installedProtocols();
    }
}
