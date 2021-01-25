/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class TxPullRequestEncodeDecodeTest
{
    @Test
    void shouldEncodeAndDecodePullRequestMessage()
    {
        // given
        DatabaseId databaseId = randomDatabaseId();
        EmbeddedChannel channel = new EmbeddedChannel( new TxPullRequestEncoder(), new TxPullRequestDecoder() );
        final long arbitraryId = 23;
        TxPullRequest sent = new TxPullRequest( arbitraryId, new StoreId( 1, 2, 3, 4, 5 ), databaseId );

        // when
        channel.writeOutbound( sent );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        TxPullRequest received = channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }
}
