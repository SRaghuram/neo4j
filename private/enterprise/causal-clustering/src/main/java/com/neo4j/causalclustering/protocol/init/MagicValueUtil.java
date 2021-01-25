/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.Objects;

import static io.netty.buffer.ByteBufUtil.writeAscii;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static java.nio.charset.StandardCharsets.US_ASCII;

final class MagicValueUtil
{
    private static final String MAGIC_VALUE = "NEO4J_CLUSTER";

    private static final ByteBuf MAGIC_VALUE_BUF = createMagicValueBuf();

    private MagicValueUtil()
    {
    }

    static ByteBuf magicValueBuf()
    {
        // return a duplicate that shares the same content but has its own reader & writer index
        return MAGIC_VALUE_BUF.duplicate();
    }

    static String readMagicValue( ByteBuf buf )
    {
        var length = MAGIC_VALUE_BUF.writerIndex();
        return buf.readCharSequence( length, US_ASCII ).toString();
    }

    static boolean isCorrectMagicValue( String value )
    {
        return Objects.equals( MAGIC_VALUE, value );
    }

    private static ByteBuf createMagicValueBuf()
    {
        // create a buffer with the magic string as ASCII
        var buf = writeAscii( UnpooledByteBufAllocator.DEFAULT, MAGIC_VALUE );
        // make the buffer ignore retain/release operations because it is static and reused for every connection
        var unreleasableBuf = unreleasableBuffer( buf );
        // make the buffer read-only because it is static/shared and should never be modified
        return unreleasableBuf.asReadOnly();
    }
}
