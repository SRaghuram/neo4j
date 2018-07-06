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
package org.neo4j.bolt.v3.messaging;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.packstream.PackedInputArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.encode;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.neo4jPack;
import static org.neo4j.helpers.collection.MapUtil.map;

class HelloMessageDecoderTest
{
    private final BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
    private final BoltMessageLogger messageLogger = mock( BoltMessageLogger.class );
    private final RequestMessageDecoder decoder = new HelloMessageDecoder( responseHandler, messageLogger );

    @Test
    void shouldReturnCorrectSignature()
    {
        assertEquals( HelloMessage.SIGNATURE, decoder.signature() );
    }

    @Test
    void shouldReturnConnectResponseHandler()
    {
        assertEquals( responseHandler, decoder.responseHandler() );
    }

    @Test
    void shouldDecodeAckFailure() throws Exception
    {
        Neo4jPack neo4jPack = neo4jPack();
        HelloMessage originalMessage = new HelloMessage( map( "user_agent", "My Driver", "user", "neo4j", "password", "secret" ) );

        PackedInputArray innput = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( innput );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );
        assertEquals( originalMessage, deserializedMessage );
    }
}
