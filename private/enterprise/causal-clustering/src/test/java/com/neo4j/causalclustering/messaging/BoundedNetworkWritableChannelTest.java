/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.fail;

class BoundedNetworkWritableChannelTest
{
    @Test
    void shouldRespectSizeLimit() throws Exception
    {
        // Given
        int sizeLimit = 100;
        BoundedNetworkWritableChannel channel = new BoundedNetworkWritableChannel( Unpooled.buffer(), sizeLimit );

        // when
        for ( int i = 0; i < sizeLimit; i++ )
        {
            channel.put( (byte) 1 );
        }

        try
        {
            channel.put( (byte) 1 );
            Assertions.fail( "Should not allow more bytes than what the limit dictates" );
        }
        catch ( MessageTooBigException e )
        {
            // then
        }
    }

    @Test
    void sizeLimitShouldWorkWithArrays() throws Exception
    {
        // Given
        int sizeLimit = 100;
        BoundedNetworkWritableChannel channel = new BoundedNetworkWritableChannel( Unpooled.buffer(), sizeLimit );

        // When
        int padding = 10;
        for ( int i = 0; i < sizeLimit - padding; i++ )
        {
            channel.put( (byte) 0 );
        }

        try
        {
            channel.put( new byte[padding * 2], padding * 2 );
            Assertions.fail( "Should not allow more bytes than what the limit dictates" );
        }
        catch ( MessageTooBigException e )
        {
            // then
        }
    }

    @Test
    void shouldNotCountBytesAlreadyInBuffer() throws Exception
    {
        // Given
        int sizeLimit = 100;
        ByteBuf buffer = Unpooled.buffer();

        int padding = Long.BYTES;
        buffer.writeLong( 0 );

        BoundedNetworkWritableChannel channel = new BoundedNetworkWritableChannel( buffer, sizeLimit );

        // When
        for ( int i = 0; i < sizeLimit - padding; i++ )
        {
            channel.put( (byte) 0 );
        }
        // then it should be ok
        // again, when
        for ( int i = 0; i < padding; i++ )
        {
            channel.put( (byte) 0 );
        }
        // then again, it should work
        // finally, when we pass the limit
        try
        {
            channel.put( (byte) 0 );
            Assertions.fail( "Should not allow more bytes than what the limit dictates" );
        }
        catch ( MessageTooBigException e )
        {
            // then
        }
    }
}
