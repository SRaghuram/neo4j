/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private final ReadWriteLock serviceLock = new ReentrantReadWriteLock();
    private final Log log;

    private boolean senderServiceRunning;

    public SenderService( ChannelInitializer channelInitializer, JobScheduler scheduler, LogProvider logProvider,
            BootstrapConfiguration<? extends SocketChannel> bootstrapConfiguration )
    {
        this.channels = new ChannelPools( bootstrapConfiguration, scheduler, channelInitializer, logProvider );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void send( AdvertisedSocketAddress to, Message message, boolean block )
    {
        ChannelFuture channelFuture;
        if ( !senderServiceRunning )
        {
            return;
        }
        try
        {
            serviceLock.readLock().lock();
            channelFuture =
                    loggingBlock( to, channels.acquire( to ).thenApply( p -> p.channel().writeAndFlush( message ).addListener( future -> p.release() ) ) );
        }
        finally
        {
            serviceLock.readLock().unlock();
        }
        if ( block )
        {
            if ( channelFuture != null )
            {
                loggingBlock( to, channelFuture );
            }
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
        serviceLock.writeLock().lock();
        try
        {
            channels.start();
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
            channels.stop();
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
