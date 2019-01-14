/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequestDecoder;
import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequestEncoder;
import org.neo4j.causalclustering.identity.StoreId;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class PrepareStoreCopyRequestMarshalTest
{
    EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new PrepareStoreCopyRequestEncoder(), new PrepareStoreCopyRequestDecoder( DEFAULT_DATABASE_NAME ) );
    }

    @Test
    public void storeIdIsTransmitted()
    {
        // given store id requests transmit store id
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        PrepareStoreCopyRequest prepareStoreCopyRequest = new PrepareStoreCopyRequest( storeId, DEFAULT_DATABASE_NAME );

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
