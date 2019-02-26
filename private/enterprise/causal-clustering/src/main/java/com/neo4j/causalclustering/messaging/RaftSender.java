/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.net.ChannelPoolService;
import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;

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
        CompletableFuture<Void> fRelease = channels.acquire( to ).thenCompose( pooledChannel -> sendMessage( pooledChannel, message ) );
        if ( block )
        {
            try
            {
                fRelease.get();
            }
            catch ( ExecutionException e )
            {
                log.error( "Exception while sending to: " + to, e );
            }
            catch ( InterruptedException e )
            {
                fRelease.cancel( true );
                log.info( "Interrupted while sending", e );
            }
        }
    }

    private CompletableFuture<Void> sendMessage( PooledChannel pooledChannel, Message message )
    {
        CompletableFuture<Void> fRelease = new CompletableFuture<>();
        fRelease.whenComplete( ( ignore, throwable ) ->
        {
            if ( throwable instanceof CancellationException )
            {
                pooledChannel.channel().close();
            }
        } );

        ChannelFuture fWrite = pooledChannel.channel().writeAndFlush( message );

        fWrite.addListener( (ChannelFutureListener) writeComplete ->
        {
            Future<Void> releaseFuture = pooledChannel.release();

            releaseFuture.addListener( releaseComplete ->
            {
                if ( !releaseComplete.isSuccess() )
                {
                    log.warn( "Failed to release channel to pool", releaseComplete.cause() );
                }
                if ( !writeComplete.isSuccess() )
                {
                    writeComplete.channel().close();
                    fRelease.completeExceptionally( writeComplete.cause() );
                }
                else
                {
                    fRelease.complete( null );
                }
            } );
        } );

        return fRelease;
    }
}
