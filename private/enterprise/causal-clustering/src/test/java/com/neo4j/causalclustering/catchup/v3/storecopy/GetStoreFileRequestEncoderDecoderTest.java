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

import java.nio.file.Path;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GetStoreFileRequestEncoderDecoderTest
{
    private static final NamedDatabaseId NAMED_DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();
    private static final StoreId expectedStore = new StoreId( 1, 2, 3, 4, 5 );
    private static final Path expectedFile = Path.of( "abc.123" );
    private static final Long expectedLastTransaction = 1234L;

    private final EmbeddedChannel embeddedChannel = new EmbeddedChannel( new GetStoreFileRequestEncoder(), new GetStoreFileRequestDecoder() );

    @AfterEach
    void tearDown()
    {
        embeddedChannel.finishAndReleaseAll();
    }

    @Test
    void getsTransmitted()
    {
        // given
        GetStoreFileRequest expectedStoreRequest =
                new GetStoreFileRequest( expectedStore, expectedFile, expectedLastTransaction, NAMED_DATABASE_ID.databaseId() );

        // when
        sendToChannel( expectedStoreRequest, embeddedChannel );

        // then
        GetStoreFileRequest actualStoreRequest = embeddedChannel.readInbound();
        assertEquals( expectedStore, actualStoreRequest.expectedStoreId() );
        assertEquals( expectedFile, actualStoreRequest.path() );
        assertEquals( expectedLastTransaction.longValue(), actualStoreRequest.requiredTransactionId() );
    }

    private static void sendToChannel( GetStoreFileRequest getStoreFileRequest, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( getStoreFileRequest );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
