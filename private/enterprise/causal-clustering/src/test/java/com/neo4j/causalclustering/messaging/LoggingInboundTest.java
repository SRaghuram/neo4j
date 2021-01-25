/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoggingInboundTest
{
    private final RaftMemberId memberId = IdFactory.randomRaftMemberId();
    private final RaftGroupId groupId = IdFactory.randomRaftId();
    private final NamedDatabaseId namedDatabaseId = DatabaseIdFactory.from( "foo", groupId.uuid() );
    private final DatabaseId databaseId = namedDatabaseId.databaseId();

    private final FakeInBound inbound = new FakeInBound();
    private final RaftMessageLogger<RaftMemberId> messageLogger = mock( RaftMessageLogger.class );
    private final DatabaseIdRepository databaseIdRepository = mock( DatabaseIdRepository.class );
    private final Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> emptyMessageHandler = message ->
    {
    };

    @Test
    void shouldWorkWithUnknownDatabase()
    {
        // given
        var loggingInbound = new LoggingInbound( inbound, messageLogger, databaseIdRepository );
        loggingInbound.registerHandler( emptyMessageHandler );

        // when
        var message = new RaftMessages.Heartbeat( memberId, 1, 1, 1 );
        inbound.handle( groupId, message );

        // then
        var unknownDatabaseId = DatabaseIdFactory.from( "unknown", groupId.uuid() );
        verify( messageLogger ).logInbound( unknownDatabaseId, memberId, message );
    }

    @Test
    void shouldWorkWithDatabase()
    {
        // given
        var loggingInbound = new LoggingInbound( inbound, messageLogger, databaseIdRepository );
        loggingInbound.registerHandler( emptyMessageHandler );

        // when
        when( databaseIdRepository.getById( databaseId ) ).thenReturn( Optional.of( namedDatabaseId ) );
        var message = new RaftMessages.Heartbeat( memberId, 1, 1, 1 );
        inbound.handle( groupId, message );

        // then
        verify( messageLogger ).logInbound( namedDatabaseId, memberId, message );
    }

    private static class FakeInBound implements Inbound<RaftMessages.InboundRaftMessageContainer<?>>
    {
        private Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> actual;

        @Override
        public void registerHandler( Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> actual )
        {
            this.actual = actual;
        }

        private void handle( RaftGroupId groupId, RaftMessages.RaftMessage message )
        {
            actual.handle( RaftMessages.InboundRaftMessageContainer.of( Instant.now(), groupId, message ) );
        }
    }

    ;
}
