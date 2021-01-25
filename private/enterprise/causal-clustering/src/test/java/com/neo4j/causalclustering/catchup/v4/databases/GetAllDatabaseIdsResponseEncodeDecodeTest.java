/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

public class GetAllDatabaseIdsResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeDecode()
    {
        //given
        final var channel = new EmbeddedChannel( new GetAllDatabaseIdsResponseEncoder(), new GetAllDatabaseIdsResponseDecoder() );
        final var databaseId1 = randomNamedDatabaseId();
        final var databaseId2 = randomNamedDatabaseId();
        final var request = new GetAllDatabaseIdsResponse( Set.of( DatabaseIdFactory.from( databaseId1.name(), databaseId1.databaseId().uuid() ),
                                                                   DatabaseIdFactory.from( databaseId2.name(), databaseId2.databaseId().uuid() ) ) );

        //when
        channel.writeOutbound( request );
        final var message = channel.readOutbound();
        channel.writeInbound( message );

        //then
        var received = channel.readInbound();
        assertNotSame( request, received );
        assertEquals( request, received );
    }
}
