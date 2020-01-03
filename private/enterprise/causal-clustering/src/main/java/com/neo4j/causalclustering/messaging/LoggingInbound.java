/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.logging.RaftMessageLogger;

public class LoggingInbound<M extends RaftMessages.RaftMessage> implements Inbound<M>
{
    private final Inbound<M> inbound;
    private final RaftMessageLogger<MemberId> raftMessageLogger;
    private final MemberId me;

    public LoggingInbound( Inbound<M> inbound, RaftMessageLogger<MemberId> raftMessageLogger, MemberId me )
    {
        this.inbound = inbound;
        this.raftMessageLogger = raftMessageLogger;
        this.me = me;
    }

    @Override
    public void registerHandler( final MessageHandler<M> handler )
    {
        inbound.registerHandler( new MessageHandler<>()
        {
            @Override
            public synchronized void handle( M message )
            {
                raftMessageLogger.logInbound( message.from(), message, me );
                handler.handle( message );
            }
        } );
    }
}
