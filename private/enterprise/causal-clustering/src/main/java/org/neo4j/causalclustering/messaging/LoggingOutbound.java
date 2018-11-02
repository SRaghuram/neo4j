/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.logging.MessageLogger;

public class LoggingOutbound<MEMBER, MESSAGE extends RaftMessages.RaftMessage> implements Outbound<MEMBER, MESSAGE>
{
    private final Outbound<MEMBER,MESSAGE> outbound;
    private final MEMBER me;
    private final MessageLogger<MEMBER> messageLogger;

    public LoggingOutbound( Outbound<MEMBER,MESSAGE> outbound, MEMBER me, MessageLogger<MEMBER> messageLogger )
    {
        this.outbound = outbound;
        this.me = me;
        this.messageLogger = messageLogger;
    }

    @Override
    public void send( MEMBER to, MESSAGE message, boolean block )
    {
        messageLogger.logOutbound( me, message, to );
        outbound.send( to, message );
    }

}
