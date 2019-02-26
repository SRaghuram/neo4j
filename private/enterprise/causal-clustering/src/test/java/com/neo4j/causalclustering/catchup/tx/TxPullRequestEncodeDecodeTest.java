/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequestDecoderV1;
import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequestEncoderV1;
import com.neo4j.causalclustering.identity.StoreId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TxPullRequestEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullRequestMessage()
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel( new TxPullRequestEncoderV1(), new TxPullRequestDecoderV1( DEFAULT_DATABASE_NAME ) );
        final long arbitraryId = 23;
        TxPullRequest sent = new TxPullRequest( arbitraryId, new StoreId( 1, 2, 3, 4 ), DEFAULT_DATABASE_NAME );

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
