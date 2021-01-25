/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

import org.neo4j.kernel.database.NamedDatabaseId;

public class LoggingOutbound<MEMBER, MESSAGE extends RaftMessages.RaftMessage> implements Outbound<MEMBER, MESSAGE>
{
    private final Outbound<MEMBER,MESSAGE> outbound;
    private final NamedDatabaseId databaseId;
    private final RaftMessageLogger<MEMBER> raftMessageLogger;

    public LoggingOutbound( Outbound<MEMBER,MESSAGE> outbound, NamedDatabaseId databaseId,
            RaftMessageLogger<MEMBER> raftMessageLogger )
    {
        this.outbound = outbound;
        this.databaseId = databaseId;
        this.raftMessageLogger = raftMessageLogger;
    }

    @Override
    public void send( MEMBER to, MESSAGE message, boolean block )
    {
        raftMessageLogger.logOutbound( databaseId, to, message );
        outbound.send( to, message );
    }
}
