/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.ssl.SslPolicy;

import static com.neo4j.causalclustering.protocol.NettyPipelineBuilder.SSL_HANDLER_NAME;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

class NettyPipelineBuilderTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final Log log = logProvider.getLog( getClass() );
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final ChannelHandlerAdapter emptyHandler = new ChannelInboundHandlerAdapter();

    @Test
    void shouldLogExceptionInbound()
    {
        // given
        RuntimeException ex = new RuntimeException();
        newServerPipelineBuilder().add( "read_handler", new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeOneInbound( new Object() );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR ).containsMessageWithException( "Exception in inbound", ex );
        assertFalse( channel.isOpen() );
    }

    @Test
    void shouldLogUnhandledMessageInbound()
    {
        // given
        Object msg = new Object();
        newServerPipelineBuilder().install();

        // when
        channel.writeOneInbound( msg );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR )
                .containsMessageWithArguments( "Unhandled inbound message: %s for channel: %s", msg, channel );
        assertFalse( channel.isOpen() );
    }

    @Test
    void shouldLogUnhandledMessageOutbound()
    {
        // given
        Object msg = new Object();
        newServerPipelineBuilder().install();

        // when
        channel.writeAndFlush( msg );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR )
                .containsMessageWithArguments( "Unhandled outbound message: %s for channel: %s", msg, channel );
        assertFalse( channel.isOpen() );
    }

    @Test
    void shouldLogExceptionOutbound()
    {
        RuntimeException ex = new RuntimeException();
        newServerPipelineBuilder().add( "write_handler", new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeAndFlush( new Object() );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR ).containsMessageWithException( "Exception in outbound", ex );
        assertFalse( channel.isOpen() );
    }

    @Test
    void shouldLogExceptionOutboundWithVoidPromise()
    {
        RuntimeException ex = new RuntimeException();
        newServerPipelineBuilder().add( "write_handler", new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeAndFlush( new Object(), channel.voidPromise() );

        // then
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR ).containsMessageWithException( "Exception in outbound", ex );
        assertFalse( channel.isOpen() );
    }

    @Test
    void shouldNotLogAnythingForHandledInbound()
    {
        // given
        Object msg = new Object();
        ChannelInboundHandlerAdapter handler = new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                // handled
            }
        };
        newServerPipelineBuilder().add( "read_handler", handler ).install();

        // when
        channel.writeOneInbound( msg );

        // then
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldNotLogAnythingForHandledOutbound()
    {
        // given
        Object msg = new Object();
        ChannelOutboundHandlerAdapter encoder = new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                ctx.write( ctx.alloc().buffer() );
            }
        };
        newServerPipelineBuilder().add( "write_handler", encoder ).install();

        // when
        channel.writeAndFlush( msg );

        // then
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldReInstallWithPreviousGate()
    {
        // given
        Object gatedMessage = new Object();

        ServerNettyPipelineBuilder builderA = newServerPipelineBuilder();
        builderA.addGate( p -> p == gatedMessage );
        builderA.install();

        assertEquals( 3, getHandlers( channel.pipeline() ).size() ); // head/tail error handlers also counted
        assertThat( channel.pipeline().names(),
                hasItems( NettyPipelineBuilder.ERROR_HANDLER_HEAD, NettyPipelineBuilder.MESSAGE_GATE_NAME,
                        NettyPipelineBuilder.ERROR_HANDLER_TAIL ) );

        // when
        ServerNettyPipelineBuilder builderB = newServerPipelineBuilder();
        builderB.add( "my_handler", emptyHandler );
        builderB.install();

        // then
        assertEquals( 4, getHandlers( channel.pipeline() ).size() ); // head/tail error handlers also counted
        assertThat( channel.pipeline().names(),
                hasItems( NettyPipelineBuilder.ERROR_HANDLER_HEAD, "my_handler", NettyPipelineBuilder.MESSAGE_GATE_NAME,
                        NettyPipelineBuilder.ERROR_HANDLER_TAIL ) );
    }

    @Test
    void shouldNotInstallSslHandlerWhenSslPolicyAbsent()
    {
        NettyPipelineBuilder.server( channel.pipeline(), null, log ).install();

        assertNull( channel.pipeline().get( SSL_HANDLER_NAME ) );
        assertNull( channel.pipeline().get( SslHandler.class ) );
    }

    @Test
    void shouldInstallSslHandlerWhenSslPolicyPresent() throws Exception
    {
        var sslHandler = new ChannelInboundHandlerAdapter();
        var sslPolicy = mock( SslPolicy.class );
        when( sslPolicy.nettyServerHandler( channel ) ).thenReturn( sslHandler );

        NettyPipelineBuilder.server( channel.pipeline(), sslPolicy, log ).install();

        assertEquals( sslHandler, channel.pipeline().get( SSL_HANDLER_NAME ) );
    }

    @Test
    void shouldKeepExistingSslHandlerWhenInstallingNewPipeline() throws Exception
    {
        var sslHandler1 = new ChannelInboundHandlerAdapter();
        var sslHandler2 = new ChannelInboundHandlerAdapter();
        var sslPolicy = mock( SslPolicy.class );
        when( sslPolicy.nettyServerHandler( channel ) ).thenReturn( sslHandler1, sslHandler2 );

        NettyPipelineBuilder.server( channel.pipeline(), sslPolicy, log ).install();
        assertEquals( sslHandler1, channel.pipeline().get( SSL_HANDLER_NAME ) );

        NettyPipelineBuilder.server( channel.pipeline(), sslPolicy, log )
                .add( "my_handler", emptyHandler )
                .install();
        assertEquals( emptyHandler, channel.pipeline().get( "my_handler" ) );
        assertEquals( sslHandler1, channel.pipeline().get( SSL_HANDLER_NAME ) );

        var allHandlers = Iterables.stream( channel.pipeline() ).map( Map.Entry::getValue ).collect( toList() );
        assertTrue( allHandlers.contains( sslHandler1 ) );
        assertFalse( allHandlers.contains( sslHandler2 ) );
    }

    private ServerNettyPipelineBuilder newServerPipelineBuilder()
    {
        return NettyPipelineBuilder.server( channel.pipeline(), null, log );
    }

    private static List<ChannelHandler> getHandlers( ChannelPipeline pipeline )
    {
        return pipeline.names().stream().map( pipeline::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
    }
}
