/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class TxStreamFinishedResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullRequestMessage()
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel(
                new TxStreamFinishedResponseEncoder(), new TxStreamFinishedResponseDecoder() );
        TxStreamFinishedResponse sent = new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 1000 );

        // when
        channel.writeOutbound( sent );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        TxStreamFinishedResponse received = channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

}
