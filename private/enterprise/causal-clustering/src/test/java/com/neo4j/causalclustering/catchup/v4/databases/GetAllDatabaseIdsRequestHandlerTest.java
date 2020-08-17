/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetAllDatabaseIdsRequestHandlerTest
{
    private final CatchupServerProtocol protocol = new CatchupServerProtocol();
    private final DatabaseManager<DatabaseContext> databaseManager = mock( DatabaseManager.class );
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void beforeEach()
    {
        var handler = new GetAllDatabaseIdsRequestHandler( protocol, databaseManager );
        channel.pipeline().addLast( handler );
    }

    @AfterEach
    void afterEach()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldGetAllDatabaseIds()
    {
        final var context = mock( StandaloneDatabaseContext.class );
        final var databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        when( databaseManager.registeredDatabases() ).thenReturn( new TreeMap<>( Map.of( databaseId, context ) ) );
        final var request = new GetAllDatabaseIdsRequest();

        assertFalse( channel.writeInbound( request ) );

        assertEquals( ResponseMessageType.ALL_DATABASE_IDS_RESPONSE, channel.readOutbound() );
        assertEquals( new GetAllDatabaseIdsResponse( Set.of( DatabaseIdFactory.from( databaseId.name(), databaseId.databaseId().uuid() ) ) ),
                      channel.readOutbound() );
        assertTrue( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }
}
