/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ChannelAttribute;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.NullLogProvider;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ClientChannelInitializerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    private final Duration timeout = Duration.ofSeconds( 42 );
    private final ClientChannelInitializer clientChannelInitializer = new ClientChannelInitializer( new NoOpChannelInitializer(),
            NettyPipelineBuilderFactory.insecure(), timeout, NullLogProvider.getInstance() );

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldRemoveItselfAfterInitialization()
    {
        channel.pipeline().addLast( clientChannelInitializer );

        assertNull( channel.pipeline().get( clientChannelInitializer.getClass() ) );
    }

    @Test
    void shouldInstallProtocolStackAttribute()
    {
        channel.pipeline().addLast( clientChannelInitializer );

        var protocolStackFuture = channel.attr( ChannelAttribute.PROTOCOL_STACK ).get();
        assertNotNull( protocolStackFuture );
        assertFalse( protocolStackFuture.isDone() );
    }

    @Test
    void shouldInstallReadTimeoutHandler()
    {
        channel.pipeline().addLast( clientChannelInitializer );

        var readTimeoutHandler = channel.pipeline().get( ReadTimeoutHandler.class );

        assertNotNull( readTimeoutHandler );
        assertEquals( timeout.toMillis(), readTimeoutHandler.getReaderIdleTimeInMillis() );
        assertEquals( 0, readTimeoutHandler.getWriterIdleTimeInMillis() );
        assertEquals( 0, readTimeoutHandler.getAllIdleTimeInMillis() );
    }

    @Test
    void shouldInstallCorrectHandlers()
    {
        channel.pipeline().addLast( clientChannelInitializer );

        var installedHandlers = Iterables.stream( channel.pipeline() )
                .map( Map.Entry::getValue )
                .collect( toList() );

        assertThat( installedHandlers, containsInRelativeOrder(
                instanceOf( ReadTimeoutHandler.class ),
                instanceOf( InitClientHandler.class ) ) );
    }
}
