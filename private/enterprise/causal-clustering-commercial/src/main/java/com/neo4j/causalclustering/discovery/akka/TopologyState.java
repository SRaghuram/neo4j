/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.Topology;
import org.neo4j.causalclustering.discovery.TopologyDifference;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;

public class TopologyState implements TopologyUpdateSink, DirectoryUpdateSink
{
    private final String localDBName;
    private final Log log;
    private final Consumer<CoreTopology> callback;
    private volatile Map<MemberId,AdvertisedSocketAddress> coreCatchupAddressMap;
    private volatile Map<MemberId,AdvertisedSocketAddress> rrCatchupAddressMap;
    private volatile Map<String, LeaderInfo> remoteDbLeaderMap;
    private volatile CoreTopology coreTopology = CoreTopology.EMPTY;
    private volatile CoreTopology localCoreTopology = CoreTopology.EMPTY;
    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile ReadReplicaTopology localReadReplicaTopology = ReadReplicaTopology.EMPTY;

    public TopologyState( Config config, LogProvider logProvider, Consumer<CoreTopology> listener )
    {
        this.localDBName = config.get( CausalClusteringSettings.database );
        this.log = logProvider.getLog( getClass() );
        this.coreCatchupAddressMap = emptyMap();
        this.rrCatchupAddressMap = emptyMap();
        this.remoteDbLeaderMap = emptyMap();
        this.callback = listener;
    }

    @Override
    public void onTopologyUpdate( CoreTopology newCoreTopology )
    {
        TopologyDifference diff = this.coreTopology.difference( newCoreTopology );
        this.coreTopology = newCoreTopology;
        this.localCoreTopology = newCoreTopology.filterTopologyByDb( localDBName );
        this.coreCatchupAddressMap = extractCatchupAddressesMap( newCoreTopology );
        if ( diff.hasChanges() )
        {
            log.info( "Core topology changed %s", diff );
            callback.accept( newCoreTopology );
        }
    }

    @Override
    public void onTopologyUpdate( ReadReplicaTopology newReadReplicaTopology )
    {
        TopologyDifference diff = this.readReplicaTopology.difference( newReadReplicaTopology );
        this.readReplicaTopology = newReadReplicaTopology;
        this.localReadReplicaTopology = newReadReplicaTopology.filterTopologyByDb( localDBName );
        this.rrCatchupAddressMap = extractCatchupAddressesMap( newReadReplicaTopology );
        if ( diff.hasChanges() )
        {
            log.info( "Read replica topology changed %s", diff );
        }
    }

    @Override
    public void onDbLeaderUpdate( Map<String, LeaderInfo> leaderInfoMap )
    {
        if ( !leaderInfoMap.equals( remoteDbLeaderMap ) )
        {
            log.info( "Database leaders changed: %s", leaderInfoMap );
            this.remoteDbLeaderMap = leaderInfoMap;
        }
    }

    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        Set<MemberId> leaders = remoteDbLeaderMap.values().stream().map( LeaderInfo::memberId ).collect( Collectors.toSet() );
        Set<MemberId> allCoreMembers = coreTopology.members().keySet();

        Function<MemberId,RoleInfo> roleMapper = m -> leaders.contains( m ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
        return allCoreMembers.stream().collect( Collectors.toMap( Function.identity(), roleMapper ) );
    }

    public String localDBName()
    {
        return localDBName;
    }

    public CoreTopology coreTopology()
    {
        return coreTopology;
    }

    public ReadReplicaTopology readReplicaTopology()
    {
        return readReplicaTopology;
    }

    public CoreTopology localCoreTopology()
    {
        return localCoreTopology;
    }

    public ReadReplicaTopology localReadReplicaTopology()
    {
        return localReadReplicaTopology;
    }

    private Map<MemberId,AdvertisedSocketAddress> extractCatchupAddressesMap( Topology<?> topology )
    {
        return topology.members().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue().getCatchupServer() ) );
    }

    public AdvertisedSocketAddress retrieveSocketAddress( MemberId memberId )
    {
        AdvertisedSocketAddress coreAddress = coreCatchupAddressMap.get( memberId );
        log.debug( "Catchup address for core %s was %s", memberId, coreAddress );
        return coreAddress != null ? coreAddress : rrCatchupAddressMap.get( memberId );
    }
}
