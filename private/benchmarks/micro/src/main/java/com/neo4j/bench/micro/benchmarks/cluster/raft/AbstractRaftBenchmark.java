/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.micro.benchmarks.cluster.EditionModuleBackedAbstractBenchmark;
import com.neo4j.bench.micro.benchmarks.cluster.LocalNetworkPlatform;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.io.ByteUnit;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;

public abstract class AbstractRaftBenchmark extends EditionModuleBackedAbstractBenchmark
{
    protected static boolean DEBUG;

    static final RaftMemberId MEMBER_ID = new RaftMemberId( UUID.randomUUID() );
    private final LocalNetworkPlatform platform = new LocalNetworkPlatform();
    private Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler;
    private RaftMessages.OutboundRaftMessageContainer<RaftMessages.RaftMessage> message;
    private Log log = logProvider().getLog( AbstractRaftBenchmark.class );

    @Override
    public String benchmarkGroup()
    {
        return "Raft";
    }

    static LogProvider logProvider()
    {
        return DEBUG ? new Log4jLogProvider( System.out, Level.DEBUG ) : NullLogProvider.getInstance();
    }

    @Override
    public void setUp() throws Throwable
    {
        LogProvider logProvider = logProvider();
        RaftProtocolInstallers serverClientContext = new RaftProtocolInstallers( protocolVersion(), message -> handler.handle( message ), logProvider );
        platform.start( serverClientContext, logProvider );
        message = initializeRaftMessage();
    }

    private Channel getChannel() throws InterruptedException
    {
        return platform.channel();
    }

    private void setHandler( Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler )
    {
        this.handler = handler;
    }

    @Override
    public void shutdown()
    {
        platform.stop();
    }

    abstract ProtocolVersion protocolVersion();

    abstract RaftMessages.OutboundRaftMessageContainer<RaftMessages.RaftMessage> initializeRaftMessage();

    static int nbrOfBytes( String size )
    {
        return (int) ByteUnit.parse( size );
    }

    void sendOneWay() throws InterruptedException, ExecutionException
    {
        ExpectingSingleMessageHandler handler = new ExpectingSingleMessageHandler();
        setHandler( handler );
        CompletableFuture<Void> handlerFuture = handler.future();
        Channel channel = getChannel();

        channel.writeAndFlush( message ).addListener( (ChannelFutureListener) future ->
        {
            if ( future.cause() != null )
            {
                log.warn( "There was an exception when trying to send the RaftMessage", future.cause() );
                handlerFuture.completeExceptionally( future.cause() );
            }
        } );

        handlerFuture.get();
    }

    static class ExpectingSingleMessageHandler extends CountingMessageHandler
    {
        ExpectingSingleMessageHandler()
        {
            super( 1 );
        }
    }
}
