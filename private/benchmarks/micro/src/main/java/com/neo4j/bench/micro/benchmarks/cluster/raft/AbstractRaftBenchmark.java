/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.micro.benchmarks.cluster.EditionModuleBackedAbstractBenchmark;
import com.neo4j.bench.micro.benchmarks.cluster.LocalNetworkPlatform;
import com.neo4j.bench.micro.benchmarks.cluster.ProtocolVersion;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.io.ByteUnit;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public abstract class AbstractRaftBenchmark extends EditionModuleBackedAbstractBenchmark
{
    private static final boolean DEBUG = false;

    static final MemberId MEMBER_ID = new MemberId( UUID.randomUUID() );
    private final LocalNetworkPlatform platform = new LocalNetworkPlatform();
    private Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> handler;
    private RaftMessages.RaftIdAwareMessage<RaftMessages.RaftMessage> message;
    private Log log = logProvider().getLog( AbstractRaftBenchmark.class );

    @Override
    public String benchmarkGroup()
    {
        return "Raft";
    }

    static LogProvider logProvider()
    {
        return DEBUG ? FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ) : NullLogProvider.getInstance();
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

    private void setHandler( Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> handler )
    {
        this.handler = handler;
    }

    @Override
    public void shutdown()
    {
        platform.stop();
    }

    abstract ProtocolVersion protocolVersion();

    abstract RaftMessages.RaftIdAwareMessage<RaftMessages.RaftMessage> initializeRaftMessage();

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
