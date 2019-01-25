/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.net.PooledChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PooledChannelTest
{
    private ChannelPool pool;

    @BeforeEach
    void setUp()
    {
        pool = mock( ChannelPool.class );
        when( pool.release( any() ) ).thenReturn( new EmbeddedChannel().newPromise().setSuccess() );
    }

    @Test
    void shouldNotBeAbleToUseChannelAfterItHasBeenReleased() throws ExecutionException, InterruptedException
    {
        PooledChannel pooledChannel = new PooledChannel( newMockedChannel(), pool );

        pooledChannel.release().get();

        IllegalStateException illegalStateException = Assertions.assertThrows( IllegalStateException.class, pooledChannel::channel );

        assertEquals( "Channel has been released back into the pool.", illegalStateException.getMessage() );
    }

    @Test
    void shouldBeAbleToUseChannelAndThenChainRelease() throws ExecutionException, InterruptedException
    {
        PooledChannel pooledChannel = new PooledChannel( newMockedChannel(), pool );

        ByteBuf msg = ByteBufAllocator.DEFAULT.heapBuffer();
        pooledChannel.channel().writeAndFlush( msg ).addListener( f -> pooledChannel.release() ).get();

        IllegalStateException illegalStateException = Assertions.assertThrows( IllegalStateException.class, pooledChannel::channel );

        assertEquals( "Channel has been released back into the pool.", illegalStateException.getMessage() );
    }

    private EmbeddedChannel newMockedChannel()
    {
        return new EmbeddedChannel();
    }
}
