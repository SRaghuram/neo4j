/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.NoSuchElementException;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.transport.BoltProtocolFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltTestUtil.assertByteBufEquals;

public class ProtocolHandshakerTest
{
    private final Channel channel = mock( Channel.class );
    private final LogProvider logProvider = NullLogProvider.getInstance();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldChooseFirstAvailableProtocol()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            // Given
            BoltProtocol protocol = newBoltProtocol( 1 );
            BoltProtocolFactory handlerFactory = newProtocolFactory( 1, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 0}, // first choice - no protocol
                    new byte[]{0, 0, 0, 1}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 0}, // third choice - no protocol
                    new byte[]{0, 0, 0, 0} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( protocol ).install();
        }
    }

    @Test
    public void shouldHandleFragmentedMessage()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            // Given
            BoltProtocol protocol = newBoltProtocol( 1 );
            BoltProtocolFactory handlerFactory = newProtocolFactory( 1, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0} ) );
            assertEquals( 0, channel.outboundMessages().size() );
            channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{(byte) 0x17, 0, 0, 0} ) );
            assertEquals( 0, channel.outboundMessages().size() );
            channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 0, 0} ) );
            assertEquals( 0, channel.outboundMessages().size() );
            channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 1, 0, 0, 0} ) );
            assertEquals( 0, channel.outboundMessages().size() );
            channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 0, 0} ) );
            assertEquals( 0, channel.outboundMessages().size() );
            channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 0} ) );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( protocol ).install();
        }
    }

    @Test
    public void shouldHandleHandshakeFollowedImmediatelyByMessage()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            // Given
            BoltProtocol protocol = newBoltProtocol( 1 );
            BoltProtocolFactory handlerFactory = newProtocolFactory( 1, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 0}, // first choice - no protocol
                    new byte[]{0, 0, 0, 1}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 0}, // third choice - no protocol
                    new byte[]{0, 0, 0, 0}, // fourth choice - no protocol
                    new byte[]{1, 2, 3, 4} ); // this is a message
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

            assertEquals( 1, channel.inboundMessages().size() );
            assertByteBufEquals( Unpooled.wrappedBuffer( new byte[]{1, 2, 3, 4} ), channel.readInbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( protocol ).install();
        }
    }

    @Test
    public void shouldHandleMaxVersionNumber()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            long maxVersionNumber = 4_294_967_295L;

            // Given
            BoltProtocol protocol = newBoltProtocol( maxVersionNumber );
            BoltProtocolFactory handlerFactory = newProtocolFactory( maxVersionNumber, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, // first choice - no protocol
                    new byte[]{0, 0, 0, 0}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 0}, // third choice - no protocol
                    new byte[]{0, 0, 0, 0} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( (int)maxVersionNumber ), channel.readOutbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( protocol ).install();
        }
    }

    @Test
    public void shouldFallbackToNoProtocolIfNoMatch()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            // Given
            BoltProtocol protocol = newBoltProtocol( 1 );
            BoltProtocolFactory handlerFactory = newProtocolFactory( 1, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 0}, // first choice - no protocol
                    new byte[]{0, 0, 0, 2}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 3}, // third choice - no protocol
                    new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 0 ), channel.readOutbound() );

            assertFalse( channel.isActive() );
            verify( protocol, never() ).install();
        }
    }

    @Test
    public void shouldRejectIfWrongPreamble()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            // Given
            BoltProtocol protocol = newBoltProtocol( 1 );
            BoltProtocolFactory handlerFactory = newProtocolFactory( 1, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0xDE, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}, // preamble
                    new byte[]{0, 0, 0, 1}, // first choice - no protocol
                    new byte[]{0, 0, 0, 2}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 3}, // third choice - no protocol
                    new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 0, channel.outboundMessages().size() );
            assertFalse( channel.isActive() );
            verify( protocol, never() ).install();
        }
    }

    @Test
    public void shouldRejectIfInsecureWhenEncryptionRequired()
    {
        try ( BoltChannel boltChannel = newBoltChannel() )
        {
            // Given
            BoltProtocol protocol = newBoltProtocol( 1 );
            BoltProtocolFactory handlerFactory = newProtocolFactory( 1, protocol );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, true, false ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 1}, // first choice - no protocol
                    new byte[]{0, 0, 0, 2}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 3}, // third choice - no protocol
                    new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 0, channel.outboundMessages().size() );
            assertFalse( channel.isActive() );
            verify( protocol, never() ).install();
        }
    }

    private BoltChannel newBoltChannel()
    {
        return new BoltChannel( "bolt-1", "bolt", channel );
    }

    private static BoltProtocol newBoltProtocol( long version )
    {
        BoltProtocol handler = mock( BoltProtocol.class );

        when( handler.version() ).thenReturn( version );

        return handler;
    }

    private static BoltProtocolFactory newProtocolFactory( long version, BoltProtocol protocol )
    {
        return ( givenVersion, channel ) -> version == givenVersion ? protocol : null;
    }
}
