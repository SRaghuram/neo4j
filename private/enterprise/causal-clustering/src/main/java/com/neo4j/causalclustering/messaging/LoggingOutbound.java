/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.kernel.database.NamedDatabaseId;

public class LoggingOutbound<MEMBER, MESSAGE extends RaftMessages.RaftMessage> implements Outbound<MEMBER, MESSAGE>
{
    private final Outbound<MEMBER,MESSAGE> outbound;
    private final NamedDatabaseId databaseId;
    private final Lazy<MEMBER> me;
    private final RaftMessageLogger<MEMBER> raftMessageLogger;

    public LoggingOutbound( Outbound<MEMBER,MESSAGE> outbound, NamedDatabaseId databaseId, Lazy<MEMBER> me,
            RaftMessageLogger<MEMBER> raftMessageLogger )
    {
        this.outbound = outbound;
        this.databaseId = databaseId;
        this.me = me;
        this.raftMessageLogger = raftMessageLogger;
    }

    @Override
    public void send( MEMBER to, MESSAGE message, boolean block )
    {
        if ( me.get() != null )
        {
            raftMessageLogger.logOutbound( databaseId, me.get(), message, to );
            outbound.send( to, message );
        }
    }
}
