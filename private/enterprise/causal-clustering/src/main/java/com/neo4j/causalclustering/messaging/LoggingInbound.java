/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;

public class LoggingInbound implements Inbound<RaftMessages.InboundRaftMessageContainer<?>>
{
    private final Inbound<RaftMessages.InboundRaftMessageContainer<?>> inbound;
    private final RaftMessageLogger<RaftMemberId> raftMessageLogger;
    private final RaftMemberId me;
    private final DatabaseIdRepository databaseIdRepository;

    public LoggingInbound( Inbound<RaftMessages.InboundRaftMessageContainer<?>> inbound, RaftMessageLogger<RaftMemberId> raftMessageLogger, RaftMemberId me,
            DatabaseIdRepository databaseIdRepository )
    {
        this.inbound = inbound;
        this.raftMessageLogger = raftMessageLogger;
        this.me = me;
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    public void registerHandler( MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler )
    {
        inbound.registerHandler( message -> {
            var databaseId = DatabaseIdFactory.from( message.raftId().uuid() );
            var namedDatabaseId = databaseIdRepository.getById( databaseId ).orElse( null );
            raftMessageLogger.logInbound( namedDatabaseId, message.message().from(), message.message(), me );
            handler.handle( message );
        } );
    }
}
