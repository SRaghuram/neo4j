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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InitMagicMessageClientHandlerTest
{
    private final NoOpChannelInitializer handshakeInitializer = new NoOpChannelInitializer();

    private final EmbeddedChannel channel = new EmbeddedChannel( newClientHandler() );

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldSendInitMagicMessageWhenChannelBecomesActive()
    {
        assertTrue( channel.isActive() );
        assertEquals( InitialMagicMessage.instance(), channel.readOutbound() );
    }

    @Test
    void shouldInstallNewPipelineWhenCorrectInitMagicMessageReceived()
    {
        channel.writeInbound( InitialMagicMessage.instance() );

        assertTrue( handshakeInitializer.invoked() );
        assertNull( channel.pipeline().get( InitMagicMessageClientHandler.class ) );
    }

    @Test
    void shouldFailWhenIncorrectInitMagicMessageReceived()
    {
        assertThrows( IllegalStateException.class, () -> channel.writeInbound( new InitialMagicMessage( "Wrong magic" ) ) );
        assertTrue( channel.closeFuture().isDone() );
    }

    private InitMagicMessageClientHandler newClientHandler()
    {
        return new InitMagicMessageClientHandler( handshakeInitializer, NettyPipelineBuilderFactory.insecure(), NullLogProvider.getInstance() );
    }
}
