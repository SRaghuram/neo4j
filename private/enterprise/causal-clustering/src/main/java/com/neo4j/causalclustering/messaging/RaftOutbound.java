/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.Inbound.MessageHandler;
import com.neo4j.causalclustering.messaging.address.UnknownAddressMonitor;

import java.time.Clock;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

public class RaftOutbound implements Outbound<MemberId,RaftMessages.RaftMessage>
{
    private final CoreTopologyService coreTopologyService;
    private final Outbound<AdvertisedSocketAddress,Message> outbound;
    private final Supplier<Optional<RaftId>> boundRaftId;
    private final UnknownAddressMonitor unknownAddressMonitor;
    private final Log log;
    private final MemberId myself;
    private final Clock clock;
    private final MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> localMessageHandler;

    public RaftOutbound( CoreTopologyService coreTopologyService, Outbound<AdvertisedSocketAddress,Message> outbound,
            MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> localMessageHandler, Supplier<Optional<RaftId>> boundRaftId,
            LogProvider logProvider, long logThresholdMillis, MemberId myself, Clock clock )
    {
        this.coreTopologyService = coreTopologyService;
        this.outbound = outbound;
        this.boundRaftId = boundRaftId;
        this.log = logProvider.getLog( getClass() );
        this.unknownAddressMonitor = new UnknownAddressMonitor( log, Clocks.systemClock(), logThresholdMillis );
        this.myself = myself;
        this.clock = clock;
        this.localMessageHandler = localMessageHandler;
    }

    @Override
    public void send( MemberId to, RaftMessages.RaftMessage message, boolean block )
    {
        Optional<RaftId> raftId = boundRaftId.get();
        if ( raftId.isEmpty() )
        {
            log.warn( "Attempting to send a message before bound to a cluster" );
            return;
        }

        if ( to.equals( myself ) )
        {
            localMessageHandler.handle( RaftMessages.ReceivedInstantRaftIdAwareMessage.of( clock.instant(), raftId.get(), message ) );
        }
        else
        {
            CoreServerInfo targetCoreInfo = coreTopologyService.allCoreServers().get( to );
            if ( targetCoreInfo != null )
            {
                outbound.send( targetCoreInfo.getRaftServer(), RaftMessages.RaftIdAwareMessage.of( raftId.get(), message ), block );
            }
            else
            {
                unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
            }
        }
    }
}
