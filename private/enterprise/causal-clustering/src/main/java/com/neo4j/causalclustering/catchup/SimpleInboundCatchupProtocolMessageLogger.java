/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import java.net.SocketAddress;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class SimpleInboundCatchupProtocolMessageLogger implements CatchupInboundEventListener
{
    private final Log log;

    public SimpleInboundCatchupProtocolMessageLogger( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void onCatchupProtocolMessage( SocketAddress remoteAddress, CatchupProtocolMessage msg )
    {
        log.info( "Handling %s [From: %s]", msg.describe(), remoteAddress );
    }

    @Override
    public void onOtherMessage( SocketAddress remoteAddress, Object msg )
    {
        log.info( "Handling unexpected message type '%s' with message '%s' from [%s]", msg.getClass().getSimpleName(), msg, remoteAddress );
    }
}
