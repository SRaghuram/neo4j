/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.magicValueBuf;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InitServerHandlerTest
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
        assertFalse( channel.writeInbound( magicValueBuf() ) );

        assertEquals( magicValueBuf(), channel.readOutbound() );
    }

    @Test
    void shouldInstallDifferentHandlersWhenCorrectMagicMessageReceived()
    {
        assertFalse( channel.writeInbound( magicValueBuf() ) );

        assertNull( channel.pipeline().get( InitServerHandler.class ) );
        assertTrue( handshakeInitializer.invoked() );
    }

    @Test
    void shouldFailWhenWrongMagicMessageReceived()
    {
        var wrongMagic = channel.alloc().buffer();
        wrongMagic.writeCharSequence( "NOT_NEO4J_CLUSTER", US_ASCII );

        assertThrows( DecoderException.class, () -> channel.writeInbound( wrongMagic ) );

        assertTrue( channel.closeFuture().isDone() );
    }

    private InitServerHandler newServerHandler()
    {
        return new InitServerHandler( handshakeInitializer, NettyPipelineBuilderFactory.insecure(), NullLogProvider.getInstance() );
    }
}
