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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
        Future<Void> fOperation = channels.acquire( to )
                .thenCompose( pooledChannel -> sendMessage( pooledChannel, message ) );

        if ( block )
        {
            try
            {
                fOperation.get();
            }
            catch ( ExecutionException e )
            {
                log.error( "Exception while sending to: " + to, e );
            }
            catch ( InterruptedException e )
            {
                fOperation.cancel( true );
                log.info( "Interrupted while sending", e );
            }
        }
    }

    private CompletableFuture<Void> sendMessage( PooledChannel pooledChannel, Message message )
    {
        CompletableFuture<Void> fOperation; // write + release
        try
        {
            fOperation = new CompletableFuture<>();
            fOperation.whenComplete( ( ignore, ex ) ->
            {
                if ( ex instanceof CancellationException )
                {
                    pooledChannel.dispose();
                }
            } );

            ChannelFuture fWrite = pooledChannel.channel().writeAndFlush( message );
            fWrite.addListener( (ChannelFutureListener) writeComplete ->
            {
                if ( !writeComplete.isSuccess() )
                {
                    pooledChannel.dispose();
                    fOperation.completeExceptionally( writeComplete.cause() );
                    return;
                }

                try
                {
                    pooledChannel.release().addListener( f -> fOperation.complete( null ) );
                }
                catch ( Throwable e )
                {
                    fOperation.complete( null );
                }
            } );
        }
        catch ( Throwable e )
        {
            pooledChannel.dispose();
            throw e;
        }

        return fOperation;
    }
}
