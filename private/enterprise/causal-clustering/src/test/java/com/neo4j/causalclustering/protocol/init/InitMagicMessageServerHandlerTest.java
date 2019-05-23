/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InitMagicMessageServerHandlerTest
{
    private final NoOpChannelInitializer handshakeInitializer = new NoOpChannelInitializer();

    private final EmbeddedChannel channel = new EmbeddedChannel( newServerHandler() );

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldSendMagicMessageWhenCorrectOneIsReceived()
    {
        assertFalse( channel.writeInbound( InitialMagicMessage.instance() ) );

        assertEquals( InitialMagicMessage.instance(), channel.readOutbound() );
    }

    @Test
    void shouldInstallDifferentHandlersWhenCorrectMagicMessageReceived()
    {
        assertFalse( channel.writeInbound( InitialMagicMessage.instance() ) );

        assertNull( channel.pipeline().get( InitMagicMessageServerHandler.class ) );
        assertTrue( handshakeInitializer.invoked() );
    }

    @Test
    void shouldFailWhenIncorrectMagicMessageReceived()
    {
        assertThrows( IllegalStateException.class, () -> channel.writeInbound( new InitialMagicMessage( "Wrong magic" ) ) );

        assertTrue( channel.closeFuture().isDone() );
    }

    private InitMagicMessageServerHandler newServerHandler()
    {
        return new InitMagicMessageServerHandler( handshakeInitializer, NettyPipelineBuilderFactory.insecure(), NullLogProvider.getInstance() );
    }
}
