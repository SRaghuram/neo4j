/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SimpleNettyChannelTest
{
    private EmbeddedChannel nettyChannel = new EmbeddedChannel();

    @Test
    void shouldWriteOnNettyChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );

        // when
        Object msg = new Object();
        Future<Void> writeComplete = channel.write( msg );

        // then
        Assertions.assertNull( nettyChannel.readOutbound() );
        Assertions.assertFalse( writeComplete.isDone() );

        // when
        nettyChannel.flush();

        // then
        Assertions.assertTrue( writeComplete.isDone() );
        Assertions.assertEquals( msg, nettyChannel.readOutbound() );
    }

    @Test
    void shouldWriteAndFlushOnNettyChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );

        // when
        Object msg = new Object();
        Future<Void> writeComplete = channel.writeAndFlush( msg );

        // then
        Assertions.assertTrue( writeComplete.isDone() );
        Assertions.assertEquals( msg, nettyChannel.readOutbound() );
    }

    @Test
    void shouldThrowWhenWritingOnDisposedChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );
        channel.dispose();

        // when then
        assertThrows( IllegalStateException.class, () -> channel.write( new Object() ) );
    }

    @Test
    void shouldThrowWhenWriteAndFlushingOnDisposedChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );
        channel.dispose();

        // when then
        assertThrows( IllegalStateException.class, () -> channel.writeAndFlush( new Object() ) );
    }
}
