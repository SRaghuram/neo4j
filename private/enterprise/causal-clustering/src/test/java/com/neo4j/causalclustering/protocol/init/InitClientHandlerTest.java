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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InitClientHandlerTest
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
        assertEquals( magicValueBuf(), channel.readOutbound() );
    }

    @Test
    void shouldInstallNewPipelineWhenCorrectInitMagicMessageReceived()
    {
        channel.writeInbound( magicValueBuf() );

        assertTrue( handshakeInitializer.invoked() );
        assertNull( channel.pipeline().get( InitClientHandler.class ) );
    }

    @Test
    void shouldFailWhenWrongInitMagicMessageReceived()
    {
        var wrongMagic = channel.alloc().buffer();
        wrongMagic.writeCharSequence( "NOT_NEO4J_CLUSTER", US_ASCII );

        assertThrows( DecoderException.class, () -> channel.writeInbound( wrongMagic ) );
        assertTrue( channel.closeFuture().isDone() );
    }

    private InitClientHandler newClientHandler()
    {
        return new InitClientHandler( handshakeInitializer, NettyPipelineBuilderFactory.insecure(), NullLogProvider.getInstance() );
    }
}
