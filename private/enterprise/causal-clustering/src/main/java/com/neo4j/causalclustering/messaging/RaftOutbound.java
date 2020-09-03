/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;
import com.neo4j.causalclustering.messaging.address.UnknownAddressMonitor;

import java.time.Clock;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftOutbound implements Outbound<RaftMemberId,RaftMessages.RaftMessage>
{
    private final CoreTopologyService coreTopologyService;
    private final Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>> outbound;
    private final Supplier<Optional<RaftGroupId>> boundRaftId;
    private final UnknownAddressMonitor unknownAddressMonitor;
    private final Log log;
    private final Lazy<RaftMemberId> myself;
    private final Clock clock;
    private final MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> localMessageHandler;

    public RaftOutbound( CoreTopologyService coreTopologyService, Outbound<SocketAddress,RaftMessages.OutboundRaftMessageContainer<?>> outbound,
                         MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> localMessageHandler, Supplier<Optional<RaftGroupId>> boundRaftGroupId,
                         LogProvider logProvider, long logThresholdMillis, Lazy<RaftMemberId> myself, Clock clock )
    {
        this.coreTopologyService = coreTopologyService;
        this.outbound = outbound;
        this.boundRaftId = boundRaftGroupId;
        this.log = logProvider.getLog( getClass() );
        this.unknownAddressMonitor = new UnknownAddressMonitor( log, clock, logThresholdMillis );
        this.myself = myself;
        this.clock = clock;
        this.localMessageHandler = localMessageHandler;
    }

    @Override
    public void send( RaftMemberId to, RaftMessages.RaftMessage message, boolean block )
    {
        Optional<RaftGroupId> raftGroupId = boundRaftId.get();
        if ( raftGroupId.isEmpty() )
        {
            log.warn( "Attempting to send a message before bound to a cluster" );
            return;
        }

        if ( to.equals( myself.get() ) )
        {
            localMessageHandler.handle( RaftMessages.InboundRaftMessageContainer.of( clock.instant(), raftGroupId.get(), message ) );
        }
        else
        {
            var raftAddress = coreTopologyService.lookupRaftAddress( to );
            if ( raftAddress != null )
            {
                outbound.send( raftAddress, RaftMessages.OutboundRaftMessageContainer.of( raftGroupId.get(), message ), block );
            }
            else
            {
                unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
            }
        }
    }
}
