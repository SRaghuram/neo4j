/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.ChannelPoolService;
import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.helpers.AdvertisedSocketAddress;
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
        CompletableFuture<Void> future = channels.acquire( to ).thenCompose( pooledChannel -> sendMessage( pooledChannel, message ) );
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
                future.cancel( true );
                log.info( "Interrupted while sending", e );
            }
        }
    }

    private CompletableFuture<Void> sendMessage( PooledChannel pooledChannel, Message message )
    {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.whenComplete( ( ignore, throwable ) ->
        {
            if ( throwable instanceof CancellationException )
            {
                pooledChannel.channel().close();
            }
        } );
        pooledChannel
                .channel()
                .writeAndFlush( message )
                .addListener( (ChannelFutureListener) writeFuture -> pooledChannel.release().addListener( releaseFuture ->
        {
            if ( !releaseFuture.isSuccess() )
            {
                log.warn( "Failed to release channel to pool", releaseFuture.cause() );
            }
            if ( !writeFuture.isSuccess() )
            {
                writeFuture.channel().close();
                completableFuture.completeExceptionally( writeFuture.cause() );
            }
            else
            {
                completableFuture.complete( null );
            }
        } ) );
        return completableFuture;
    }
}
