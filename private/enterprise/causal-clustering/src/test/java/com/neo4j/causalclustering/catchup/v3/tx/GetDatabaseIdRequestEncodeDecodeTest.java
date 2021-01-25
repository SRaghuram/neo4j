/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequest;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequestEncoder;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class GetDatabaseIdRequestEncodeDecodeTest
{
    @Test
    void shouldEncodeDecode()
    {
        // given
        var channel = new EmbeddedChannel( new GetDatabaseIdRequestEncoder(), new GetDatabaseIdRequestDecoder() );
        var sent = new GetDatabaseIdRequest( "database name" );

        // when
        channel.writeOutbound( sent );
        var message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        var received = channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );

    }
}
