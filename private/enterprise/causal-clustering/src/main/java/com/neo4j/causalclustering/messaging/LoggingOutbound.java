/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

public class LoggingOutbound<MEMBER, MESSAGE extends RaftMessages.RaftMessage> implements Outbound<MEMBER, MESSAGE>
{
    private final Outbound<MEMBER,MESSAGE> outbound;
    private final MEMBER me;
    private final RaftMessageLogger<MEMBER> raftMessageLogger;

    public LoggingOutbound( Outbound<MEMBER,MESSAGE> outbound, MEMBER me, RaftMessageLogger<MEMBER> raftMessageLogger )
    {
        this.outbound = outbound;
        this.me = me;
        this.raftMessageLogger = raftMessageLogger;
    }

    @Override
    public void send( MEMBER to, MESSAGE message, boolean block )
    {
        raftMessageLogger.logOutbound( me, message, to );
        outbound.send( to, message );
    }

}
