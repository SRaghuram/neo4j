/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.protocol.handshake.GateEvent;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MessageGateTest
{
    private final String ALLOWED_MSG = "allowed";
    private final MessageGate gate = new MessageGate( m -> m != ALLOWED_MSG );
    private final ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
    private final Channel channel = mock( Channel.class );
    private final ChannelPipeline pipeline = mock( ChannelPipeline.class );

    @Before
    public void setup()
    {
        when( channel.pipeline() ).thenReturn( pipeline );
        when( ctx.channel() ).thenReturn( channel );
    }

    @Test
    public void shouldLetAllowedMessagesPass() throws Exception
    {
        // when
        ChannelPromise promise = mock( ChannelPromise.class );
        gate.write( ctx, ALLOWED_MSG, promise );
        gate.write( ctx, ALLOWED_MSG, promise );
        gate.write( ctx, ALLOWED_MSG, promise );

        // then
        verify( ctx, times( 3 ) ).write( ALLOWED_MSG, promise );
    }

    @Test
    public void shouldGateMessages() throws Exception
    {
        // when
        ChannelPromise promise = mock( ChannelPromise.class );
        gate.write( ctx, "A", promise );
        gate.write( ctx, "B", promise );
        gate.write( ctx, "C", promise );

        // then
        verify( ctx, never() ).write( any(), any() );
    }

    @Test
    public void shouldLetGatedMessagesPassOnSuccess() throws Exception
    {
        // given
        ChannelPromise promiseA = mock( ChannelPromise.class );
        ChannelPromise promiseB = mock( ChannelPromise.class );
        ChannelPromise promiseC = mock( ChannelPromise.class );

        gate.write( ctx, "A", promiseA );
        gate.write( ctx, "B", promiseB );
        gate.write( ctx, "C", promiseC );
        verify( ctx, never() ).write( any(), any() );

        // when
        gate.userEventTriggered( ctx, GateEvent.getSuccess() );

        // then
        InOrder inOrder = Mockito.inOrder( ctx );
        inOrder.verify( ctx ).write( "A", promiseA );
        inOrder.verify( ctx ).write( "B", promiseB );
        inOrder.verify( ctx ).write( "C", promiseC );
        inOrder.verify( ctx, never() ).write( any(), any() );
    }

    @Test
    public void shouldRemoveGateOnSuccess() throws Exception
    {
        // when
        gate.userEventTriggered( ctx, GateEvent.getSuccess() );

        // then
        verify( pipeline ).remove( gate );
    }

    @Test
    public void shouldNotLetGatedMessagesPassAfterFailure() throws Exception
    {
        // given
        ChannelPromise promise = mock( ChannelPromise.class );
        gate.userEventTriggered( ctx, GateEvent.getFailure() );

        // when
        gate.write( ctx, "A", promise );
        gate.write( ctx, "B", promise );
        gate.write( ctx, "C", promise );

        // then
        verify( ctx, never() ).write( any(), any() );
    }

    @Test
    public void shouldStillLetAllowedMessagePassAfterFailure() throws Exception
    {
        // given
        ChannelPromise promise = mock( ChannelPromise.class );
        gate.userEventTriggered( ctx, GateEvent.getFailure() );

        // when
        gate.write( ctx, ALLOWED_MSG, promise );

        // then
        verify( ctx ).write( ALLOWED_MSG, promise );
    }

    @Test
    public void shouldLeaveGateOnFailure() throws Exception
    {
        // when
        gate.userEventTriggered( ctx, GateEvent.getFailure() );

        // then
        verify( pipeline, never() ).remove( any( ChannelHandler.class ) );
    }
}
