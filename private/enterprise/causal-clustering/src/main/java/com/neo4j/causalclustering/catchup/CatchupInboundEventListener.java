/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.net.SocketAddress;

public interface CatchupInboundEventListener
{
    CatchupInboundEventListener NO_OP = new CatchupInboundEventListener()
    {
        @Override
        public void onCatchupProtocolMessage( SocketAddress remoteAddress, CatchupProtocolMessage msg ) {}

        @Override
        public void onOtherMessage( SocketAddress remoteAddress, Object msg ) {}
    };

    default void onTxPullRequest( SocketAddress remoteAddress, TxPullRequest msg )
    {
        onCatchupProtocolMessage( remoteAddress, msg );
    }

    void onCatchupProtocolMessage( SocketAddress remoteAddress, CatchupProtocolMessage msg );

    void onOtherMessage( SocketAddress remoteAddress, Object msg );
}
