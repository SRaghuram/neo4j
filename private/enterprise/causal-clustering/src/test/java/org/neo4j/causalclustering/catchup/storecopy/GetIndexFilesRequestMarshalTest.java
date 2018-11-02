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

public class GetIndexFilesRequestMarshalTest
{
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new GetIndexFilesRequest.Encoder(), new GetIndexFilesRequest.Decoder() );
    }

    private static final StoreId expectedStore = new StoreId( 1, 2, 3, 4 );
    private static final long exepctedIndexId = 13;
    private static final Long expectedLastTransaction = 1234L;

    @Test
    public void getsTransmitted()
    {
        // given
        GetIndexFilesRequest expectedIndexSnapshotRequest = new GetIndexFilesRequest( expectedStore, exepctedIndexId, expectedLastTransaction );

        // when
        sendToChannel( expectedIndexSnapshotRequest, embeddedChannel );

        // then
        GetIndexFilesRequest actualIndexRequest = embeddedChannel.readInbound();
        assertEquals( expectedStore, actualIndexRequest.expectedStoreId() );
        assertEquals( exepctedIndexId, actualIndexRequest.indexId() );
        assertEquals( expectedLastTransaction.longValue(), actualIndexRequest.requiredTransactionId() );
    }

    private static void sendToChannel( GetIndexFilesRequest expectedIndexSnapshotRequest, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( expectedIndexSnapshotRequest );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
