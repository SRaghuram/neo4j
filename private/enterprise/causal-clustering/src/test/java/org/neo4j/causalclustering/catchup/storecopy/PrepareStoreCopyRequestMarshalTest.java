/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.identity.StoreId;

import static org.junit.Assert.assertEquals;

public class PrepareStoreCopyRequestMarshalTest
{
    EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new PrepareStoreCopyRequestEncoder(), new PrepareStoreCopyRequestDecoder() );
    }

    @Test
    public void storeIdIsTransmitted()
    {
        // given store id requests transmit store id
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        PrepareStoreCopyRequest prepareStoreCopyRequest = new PrepareStoreCopyRequest( storeId );

        // when transmitted
        sendToChannel( prepareStoreCopyRequest, embeddedChannel );

        // then it can be received/deserialised
        PrepareStoreCopyRequest prepareStoreCopyRequestRead = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyRequest.getStoreId(), prepareStoreCopyRequestRead.getStoreId() );
    }

    public static <E> void sendToChannel( E e, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( e );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
