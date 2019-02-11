/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.ChannelPoolService;
import com.neo4j.causalclustering.net.PooledChannel;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.AbstractChannelPoolHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftSender implements Outbound<AdvertisedSocketAddress,Message>
{
    private final ChannelPoolService channels;
    private final Log log;

    public RaftSender( LogProvider logProvider, RaftChannelPoolService channelPoolService )
    {
        this.channels = channelPoolService;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void send( AdvertisedSocketAddress to, Message message, boolean block )
    {
        PooledChannel pooledChannel = loggingBlock( to, channels.acquire( to ) );
        if ( pooledChannel == null )
        {
            return;
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
}
