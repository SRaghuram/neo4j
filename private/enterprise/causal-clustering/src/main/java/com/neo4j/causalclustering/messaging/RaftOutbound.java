/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.address.UnknownAddressMonitor;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

public class RaftOutbound implements Outbound<MemberId,RaftMessages.RaftMessage>
{
    private final CoreTopologyService coreTopologyService;
    private final Outbound<AdvertisedSocketAddress,Message> outbound;
    private final Supplier<Optional<ClusterId>> clusterIdentity;
    private final UnknownAddressMonitor unknownAddressMonitor;
    private final Log log;

    public RaftOutbound( CoreTopologyService coreTopologyService, Outbound<AdvertisedSocketAddress,Message> outbound,
                         Supplier<Optional<ClusterId>> clusterIdentity, LogProvider logProvider, long logThresholdMillis )
    {
        this.coreTopologyService = coreTopologyService;
        this.outbound = outbound;
        this.clusterIdentity = clusterIdentity;
        this.log = logProvider.getLog( getClass() );
        this.unknownAddressMonitor = new UnknownAddressMonitor( log, Clocks.systemClock(), logThresholdMillis );
    }

    @Override
    public void send( MemberId to, RaftMessages.RaftMessage message, boolean block )
    {
        Optional<ClusterId> clusterId = clusterIdentity.get();
        if ( !clusterId.isPresent() )
        {
            log.warn( "Attempting to send a message before bound to a cluster" );
            return;
        }

        Optional<CoreServerInfo> coreServerInfo = coreTopologyService.localCoreServers().find( to );
        if ( coreServerInfo.isPresent() )
        {
            outbound.send( coreServerInfo.get().getRaftServer(), RaftMessages.ClusterIdAwareMessage.of( clusterId.get(), message ), block );
        }
        else
        {
            unknownAddressMonitor.logAttemptToSendToMemberWithNoKnownAddress( to );
        }
    }
}
