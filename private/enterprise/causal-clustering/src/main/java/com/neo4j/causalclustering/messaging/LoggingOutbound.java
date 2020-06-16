/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.kernel.database.NamedDatabaseId;

public class LoggingOutbound<MEMBER, MESSAGE extends RaftMessages.RaftMessage> implements Outbound<MEMBER, MESSAGE>
{
    private final Outbound<MEMBER,MESSAGE> outbound;
    private final NamedDatabaseId databaseId;
    private final MEMBER me;
    private final RaftMessageLogger<MEMBER> raftMessageLogger;

    public LoggingOutbound( Outbound<MEMBER,MESSAGE> outbound, NamedDatabaseId databaseId, MEMBER me, RaftMessageLogger<MEMBER> raftMessageLogger )
    {
        this.outbound = outbound;
        this.databaseId = databaseId;
        this.me = me;
        this.raftMessageLogger = raftMessageLogger;
    }

    @Override
    public void send( MEMBER to, MESSAGE message, boolean block )
    {
        raftMessageLogger.logOutbound( databaseId, me, message, to );
        outbound.send( to, message );
    }

}
