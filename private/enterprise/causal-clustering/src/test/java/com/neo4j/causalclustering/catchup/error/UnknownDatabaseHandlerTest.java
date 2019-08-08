/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.error;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class UnknownDatabaseHandlerTest
{
    private final DatabaseId databaseId = new TestDatabaseIdRepository().get( "orders" );
    private final CatchupProtocolMessage.WithDatabaseId message = new CoreSnapshotRequest( databaseId );
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final CatchupServerProtocol protocol = new CatchupServerProtocol();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @BeforeEach
    void setUp()
    {
        protocol.expect( CatchupServerProtocol.State.GET_CORE_SNAPSHOT );
        var handler = new UnknownDatabaseHandler<>( message.getClass(), protocol, logProvider );
        channel.pipeline().addLast( handler );
    }

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldLogWarning()
    {
        channel.writeInbound( message );
        logProvider.assertAtLeastOnce( inLog( UnknownDatabaseHandler.class )
                .warn( containsString( "database " + databaseId.name() + " does not exist" ) ) );
    }

    @Test
    void shouldWriteErrorResponse()
    {
        channel.writeInbound( message );

        assertEquals( ResponseMessageType.ERROR, channel.readOutbound() );
        CatchupErrorResponse response = channel.readOutbound();
        assertEquals( CatchupResult.E_DATABASE_UNKNOWN, response.status() );
        assertThat( response.message(), containsString( "database " + databaseId.name() + " does not exist" ) );
    }

    @Test
    void shouldUpdateProtocolExpectation()
    {
        channel.writeInbound( message );
        assertTrue( protocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }
}
