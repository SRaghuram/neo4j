/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class InitMagicMessageEncoderDecoderTest
{
    @Inject
    private RandomRule random;

    private final EmbeddedChannel channel = new EmbeddedChannel( new InitMagicMessageEncoder(),
            new InitMagicMessageDecoder( NullLogProvider.getInstance() ) );

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldDecodeFromSingleBuffer()
    {
        var originalMessage = new InitialMagicMessage( random.nextAlphaNumericString() );

        assertTrue( channel.writeOutbound( originalMessage ) );
        var buf = (ByteBuf) channel.readOutbound();

        assertTrue( channel.writeInbound( buf ) );
        var decodedMessage = channel.readInbound();
        assertNotSame( originalMessage, decodedMessage );
        assertEquals( originalMessage, decodedMessage );
    }

    @Test
    void shouldDecodeFromMultipleBuffers()
    {
        testDecodingFromMultipleBuffers( this::sliceIntoMultipleSingleByteBuffers );
    }

    @Test
    void shouldDecodeFromTwoBuffers()
    {
        testDecodingFromMultipleBuffers( this::randomlySliceIntoTwoByteBuffers );
    }

    @Test
    void shouldFailOnIncorrectMessageCode()
    {
        var message = new InitialMagicMessage( random.nextAlphaNumericString() );
        assertTrue( channel.writeOutbound( message ) );
        var buf = (ByteBuf) channel.readOutbound();

        buf.setInt( 0, 42 ); // set an incorrect message code

        // error is thrown
        assertThrows( DecoderException.class, () -> channel.writeInbound( buf ) );
        // and channel is closed
        assertTrue( channel.closeFuture().isDone() );
    }

    private void testDecodingFromMultipleBuffers( Function<ByteBuf,List<ByteBuf>> slicer )
    {
        var originalMessage = new InitialMagicMessage( random.nextAlphaNumericString() );

        assertTrue( channel.writeOutbound( originalMessage ) );
        var buf = (ByteBuf) channel.readOutbound();

        var slicedBuffs = slicer.apply( buf );

        for ( var i = 0; i < slicedBuffs.size(); i++ )
        {
            var messageDecoded = channel.writeInbound( slicedBuffs.get( i ) );

            // a full message should be decoded only when the final buffer arrives
            if ( i == slicedBuffs.size() - 1 )
            {
                assertTrue( messageDecoded );
            }
            else
            {
                assertFalse( messageDecoded );
            }
        }

        var decodedMessage = channel.readInbound();
        assertNotSame( originalMessage, decodedMessage );
        assertEquals( originalMessage, decodedMessage );
    }

    private List<ByteBuf> sliceIntoMultipleSingleByteBuffers( ByteBuf buffer )
    {
        var result = new ArrayList<ByteBuf>();
        buffer.forEachByte( value ->
        {
            var singleByteBuffer = channel.alloc().buffer( 1 );
            singleByteBuffer.writeByte( value );
            result.add( singleByteBuffer );
            return true;
        } );
        return result;
    }

    private List<ByteBuf> randomlySliceIntoTwoByteBuffers( ByteBuf buffer )
    {
        var bufferSize = buffer.writerIndex();
        var slicePoint = random.nextInt( 1, bufferSize - 1 );
        var head = buffer.retainedSlice( 0, slicePoint );
        var tail = buffer.retainedSlice( slicePoint, bufferSize - slicePoint );
        return List.of( head, tail );
    }
}
