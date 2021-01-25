/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

import java.util.UUID;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public class LoggingInbound implements Inbound<RaftMessages.InboundRaftMessageContainer<?>>
{
    private final Inbound<RaftMessages.InboundRaftMessageContainer<?>> inbound;
    private final RaftMessageLogger<RaftMemberId> raftMessageLogger;
    private final DatabaseIdRepository databaseIdRepository;

    public LoggingInbound( Inbound<RaftMessages.InboundRaftMessageContainer<?>> inbound, RaftMessageLogger<RaftMemberId> raftMessageLogger,
            DatabaseIdRepository databaseIdRepository )
    {
        this.inbound = inbound;
        this.raftMessageLogger = raftMessageLogger;
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    public void registerHandler( MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler )
    {
        inbound.registerHandler( message -> handler.handle( log( message ) ) );
    }

    private RaftMessages.InboundRaftMessageContainer<?> log( RaftMessages.InboundRaftMessageContainer<?> message )
    {
        var namedDatabaseId = resolveDatabase( message.raftGroupId().uuid() );
        raftMessageLogger.logInbound( namedDatabaseId, message.message().from(), message.message() );
        return message;
    }

    private NamedDatabaseId resolveDatabase( UUID uuid )
    {
        try
        {
            return databaseIdRepository.getById( DatabaseIdFactory.from( uuid ) ).orElse( unknownDatabase( uuid ) );
        }
        catch ( DatabaseShutdownException e )
        {
            return unknownDatabase( uuid );
        }
    }

    private static NamedDatabaseId unknownDatabase( UUID uuid )
    {
        return DatabaseIdFactory.from( "unknown", uuid );
    }
}
