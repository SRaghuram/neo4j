/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AsciiString;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.isCorrectMagicValue;
import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.magicValueBuf;
import static com.neo4j.causalclustering.protocol.init.MagicValueUtil.readMagicValue;
import static io.netty.util.ReferenceCountUtil.release;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MagicValueUtilTest
{
    @Test
    void shouldReturnMagicValueBuf()
    {
        var buf = magicValueBuf();

        try
        {
            assertEquals( new AsciiString( "NEO4J_CLUSTER" ), buf.readCharSequence( 13, US_ASCII ) );
        }
        finally
        {
            release( buf );
        }
    }

    @Test
    void shouldReturnMagicValueBuffsWithCorrectReaderIndexes()
    {
        var buf1 = magicValueBuf();
        var buf2 = (ByteBuf) null;

        try
        {
            assertEquals( 0, buf1.readerIndex() );
            buf1.readerIndex( 5 );
            assertEquals( 5, buf1.readerIndex() );

            buf2 = magicValueBuf();
            assertNotSame( buf1, buf2 );
            assertEquals( 5, buf1.readerIndex() );
            assertEquals( 0, buf2.readerIndex() );
        }
        finally
        {
            release( buf1 );
            release( buf2 );
        }
    }

    @Test
    void shouldReadCorrectMagicValue()
    {
        var buf = Unpooled.buffer();

        try
        {
            buf.writeCharSequence( "NEO4J_CLUSTER", US_ASCII );
            assertEquals( "NEO4J_CLUSTER", readMagicValue( buf ) );
        }
        finally
        {
            release( buf );
        }
    }

    @Test
    void shouldReadWrongMagicValue()
    {
        var buf = Unpooled.buffer();

        try
        {
            buf.writeCharSequence( "NEO5J_MACHINE", US_ASCII );
            assertEquals( "NEO5J_MACHINE", readMagicValue( buf ) );
        }
        finally
        {
            release( buf );
        }
    }

    @Test
    void shouldThrowWhenUnableToReadMagicValue()
    {
        var buf = Unpooled.buffer( 5, 5 );

        try
        {
            buf.writeCharSequence( "HELLO", US_ASCII );
            assertThrows( IndexOutOfBoundsException.class, () -> readMagicValue( buf ) );
        }
        finally
        {
            release( buf );
        }
    }

    @Test
    void shouldCheckMagicValuesForCorrectness()
    {
        assertTrue( isCorrectMagicValue( "NEO4J_CLUSTER" ) );

        assertFalse( isCorrectMagicValue( "neo4j_cluster" ) );
        assertFalse( isCorrectMagicValue( "Neo4j_Cluster" ) );
        assertFalse( isCorrectMagicValue( "NEO5J_CLUSTER" ) );
        assertFalse( isCorrectMagicValue( "NEO___CLUSTER" ) );
        assertFalse( isCorrectMagicValue( "JUST_WRONG" ) );
    }
}
