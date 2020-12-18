/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.net.LoadBalancedTrackingChannelPoolMap.RaftGroupSocket;
import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftSender implements Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>>
{
    private final RaftChannelPoolService dataChannels;
    private final RaftChannelPoolService controlChannels;
    private final Log log;

    public RaftSender( LogProvider logProvider, RaftChannelPoolService dataChannelsPoolService, RaftChannelPoolService controlChannelsPoolService )
    {
        this.dataChannels = dataChannelsPoolService;
        this.controlChannels = controlChannelsPoolService;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void send( SocketAddress to, RaftMessages.OutboundRaftMessageContainer<?> message, boolean block )
    {
        var raftGroupSocket = new RaftGroupSocket( message.raftGroupId(), to );
        var raftMessage = message.message();
        CompletableFuture<Void> fOperation;

        if ( raftMessage.containsData() || raftMessage.requiresOrdering() )
        {
            fOperation = tryDataChannel( raftGroupSocket, message );
            if ( fOperation == null )
            {
                return;
            }
        }
        else
        {
            fOperation = useControlChannel( raftGroupSocket, message );
        }

        if ( block )
        {
            try
            {
                fOperation.get();
            }
            catch ( ExecutionException e )
            {
                log.error( "Exception while sending " + printAddress( to ), e );
            }
            catch ( InterruptedException e )
            {
                fOperation.cancel( true );
                log.info( "Interrupted while sending " + printAddress( to ), e );
            }
        }
        else
        {
            fOperation.whenComplete( ( ignore, throwable ) ->
            {
                if ( throwable != null )
                {
                    log.warn( "Raft sender failed exceptionally " + printAddress( to ), throwable );
                }
            } );
        }
    }

    private CompletableFuture<Void> useControlChannel( RaftGroupSocket raftGroupSocket, RaftMessages.OutboundRaftMessageContainer<?> message )
    {
        return controlChannels.acquire( raftGroupSocket )
                              .thenCompose( pooledChannel -> sendMessage( pooledChannel, message ) );
    }

    private CompletableFuture<Void> tryDataChannel( RaftGroupSocket raftGroupSocket, RaftMessages.OutboundRaftMessageContainer<?> message )
    {
        // Wait for channel because Raft relies on some messages being sent in order on the channel, otherwise they may have to be re-sent.
        var pooledChannel = waitForPooledDataChannel( raftGroupSocket );
        if ( pooledChannel == null )
        {
            return null;
        }
        else
        {
            return sendMessage( pooledChannel, message );
        }
    }

    private PooledChannel waitForPooledDataChannel( RaftGroupSocket to )
    {
        try
        {
            return dataChannels.acquire( to ).get();
        }
        catch ( InterruptedException e )
        {
            log.info( "Failed to acquire channel because the request was interrupted " + printAddress( to.socketAddress() ), e );
        }
        catch ( ExecutionException e )
        {
            log.warn( "Failed to acquire channel " + printAddress( to.socketAddress() ), e );
        }
        return null;
    }

    private static String printAddress( SocketAddress to )
    {
        return "[Address: " + to + "]";
    }

    private CompletableFuture<Void> sendMessage( PooledChannel pooledChannel, RaftMessages.OutboundRaftMessageContainer<?> message )
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
                    fOperation.completeExceptionally( wrapCause( pooledChannel, writeComplete.cause() ) );
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
            throw wrapCause( pooledChannel, e );
        }

        return fOperation;
    }

    private CompletionException wrapCause( PooledChannel pooledChannel, Throwable e )
    {
        return new CompletionException( "[ChannelId: " + pooledChannel.channel().id() + "]", e );
    }
}
