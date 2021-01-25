/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CatchupErrorResponseEncoderDecoderTest
{
    @Test
    void shouldEncodeDecode()
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel( new CatchupErrorResponseEncoder(), new CatchupErrorResponseDecoder() );
        CatchupErrorResponse sent = new CatchupErrorResponse( CatchupResult.E_DATABASE_UNKNOWN, "no db" );

        // when
        channel.writeOutbound( sent );
        Object byteBuf = channel.readOutbound();
        channel.writeInbound( byteBuf );
        CatchupErrorResponse received = channel.readInbound();

        // then
        assertEquals( sent.status(), received.status() );
        assertEquals( sent.message(), received.message() );
    }
}
