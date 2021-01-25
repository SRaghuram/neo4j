/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Iterables;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class ServerChannelInitializerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    private final Duration timeout = Duration.ofSeconds( 42 );
    private final ServerChannelInitializer serverChannelInitializer = new ServerChannelInitializer( new NoOpChannelInitializer(),
            NettyPipelineBuilderFactory.insecure(), timeout, nullLogProvider(), Config.defaults() );
    private final ChannelInitializer<Channel> nettyServerChannelInitializer = serverChannelInitializer.asChannelInitializer();

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldRemoveItselfAfterInitialization()
    {
        channel.pipeline().addLast( nettyServerChannelInitializer );

        assertThat( channel.pipeline(), not( contains( nettyServerChannelInitializer ) ) );
    }

    @Test
    void shouldInstallReadTimeoutHandler()
    {
        channel.pipeline().addLast( nettyServerChannelInitializer );

        var readTimeoutHandler = channel.pipeline().get( ReadTimeoutHandler.class );

        assertNotNull( readTimeoutHandler );
        assertEquals( timeout.toMillis(), readTimeoutHandler.getReaderIdleTimeInMillis() );
        assertEquals( 0, readTimeoutHandler.getWriterIdleTimeInMillis() );
        assertEquals( 0, readTimeoutHandler.getAllIdleTimeInMillis() );
    }

    @Test
    void shouldInstallCorrectHandlers()
    {
        channel.pipeline().addLast( nettyServerChannelInitializer );

        var installedHandlers = Iterables.stream( channel.pipeline() )
                .map( Map.Entry::getValue )
                .collect( toList() );

        assertThat( installedHandlers, containsInRelativeOrder(
                instanceOf( ReadTimeoutHandler.class ),
                instanceOf( InitServerHandler.class ) ) );
    }
}
