/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrepareStoreCopyRequestEncoderDecoderTest
{
    private final EmbeddedChannel embeddedChannel = new EmbeddedChannel( new PrepareStoreCopyRequestEncoder(), new PrepareStoreCopyRequestDecoder() );

    @AfterEach
    void tearDown()
    {
        embeddedChannel.finishAndReleaseAll();
    }

    @Test
    void storeIdIsTransmitted()
    {
        // given store id requests transmit store id
        StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
        PrepareStoreCopyRequest prepareStoreCopyRequest = new PrepareStoreCopyRequest( storeId, new TestDatabaseIdRepository().defaultDatabase().databaseId() );

        // when transmitted
        sendToChannel( prepareStoreCopyRequest, embeddedChannel );

        // then it can be received/deserialised
        PrepareStoreCopyRequest prepareStoreCopyRequestRead = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyRequest.storeId(), prepareStoreCopyRequestRead.storeId() );
    }

    private static <E> void sendToChannel( E e, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( e );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
