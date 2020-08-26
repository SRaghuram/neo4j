/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.procedure.builtin.routing.RoutingResult;

import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class AddressCollector
{
    private final TopologyService topologyService;
    private final LeaderService leaderService;
    private final Config config;
    private final Log log;

    public AddressCollector( TopologyService topologyService, LeaderService leaderService, Config config, Log log )
    {
        this.topologyService = topologyService;
        this.leaderService = leaderService;
        this.config = config;
        this.log = log;
    }

    public RoutingResult createRoutingResult( NamedDatabaseId namedDatabaseId )
    {
        return  createRoutingResult( namedDatabaseId, null );
    }

    public RoutingResult createRoutingResult( NamedDatabaseId namedDatabaseId, Policy policy )
    {
        var coreTopology = topologyService.coreTopologyForDatabase( namedDatabaseId );
        var rrTopology = topologyService.readReplicaTopologyForDatabase( namedDatabaseId );
        var optionalLeaderId = leaderService.getLeaderServer( namedDatabaseId );

        var timeToLive = config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
        var shouldShuffle = config.get( CausalClusteringSettings.load_balancing_shuffle );

        return new RoutingResult( routeEndpoints( coreTopology, policy, shouldShuffle ),
                                  writeEndpoints( namedDatabaseId ),
                                  readEndpoints( coreTopology, rrTopology, optionalLeaderId, policy, shouldShuffle, namedDatabaseId ),
                                  timeToLive );
    }

    private List<SocketAddress> routeEndpoints( DatabaseCoreTopology coreTopology, Policy policy, boolean shouldShuffle )
    {
        var routers = coreTopology.servers().entrySet().stream().map( AddressCollector::newServerInfo );

        if ( policy != null )
        {
            var routersSet = routers.collect( toSet() );

            var preferredRouters = new ArrayList<>( policy.apply( routersSet ) );
            routersSet.removeAll( preferredRouters );
            var otherRouters = new ArrayList<>( routersSet );

            if ( shouldShuffle )
            {
                Collections.shuffle( preferredRouters );
                Collections.shuffle( otherRouters );
            }
            routers = Stream.concat( preferredRouters.stream(), otherRouters.stream() );
        }
        else
        {
            var allRouters = routers.collect( toList() );
            Collections.shuffle( allRouters );
            routers = allRouters.stream();
        }
        return routers.map( ServerInfo::boltAddress ).collect( toList() );
    }

    private List<SocketAddress> writeEndpoints( NamedDatabaseId namedDatabaseId )
    {
        var optionalLeaderAddress = leaderService.getLeaderBoltAddress( namedDatabaseId );
        if ( optionalLeaderAddress.isEmpty() )
        {
            log.debug( "No leader server found. This can happen during a leader switch. No write end points available" );
        }
        return optionalLeaderAddress.stream().collect( toList() );
    }

    private List<SocketAddress> readEndpoints( DatabaseCoreTopology coreTopology,
                                               DatabaseReadReplicaTopology rrTopology,
                                               Optional<MemberId> optionalLeaderId,
                                               Policy policy,
                                               boolean shouldShuffle,
                                               NamedDatabaseId dbId )
    {
        var possibleReaders = rrTopology.servers().entrySet().stream().map( AddressCollector::newServerInfo ).collect( toSet() );

        if ( config.get( cluster_allow_reads_on_followers ) || possibleReaders.isEmpty() )
        {
            var coreMembers = coreTopology.servers().entrySet().stream().map( AddressCollector::newServerInfo ).collect( toSet() );

            // if the leader is present and it is not alone filter it out from the read end points
            if ( optionalLeaderId.isPresent() && coreMembers.size() > 1 )
            {
                var leaderId = optionalLeaderId.get();
                coreMembers = coreMembers.stream().filter( serverInfo -> !serverInfo.memberId().equals( leaderId ) ).collect( toSet() );
            }
            // if there is only the leader return it as read end point
            // or if we cannot locate the leader return all cores as read end points
            possibleReaders.addAll( coreMembers );
            // leader might become available a bit later and we might end up using it for reading during this ttl, should be fine in general
        }

        var readers = new ArrayList<>( policy == null ? possibleReaders :
                                       policy.apply( possibleReaders ) );

        if ( shouldShuffle || policy == null )
        {
            Collections.shuffle( readers );
        }
        return readers.stream()
                      .filter( r -> isMemberOnline( r.memberId(), dbId ) )
                      .map( ServerInfo::boltAddress ).collect( toList() );
    }

    private boolean isMemberOnline( MemberId memberId, NamedDatabaseId dbId )
    {
        return topologyService.lookupDatabaseState( dbId, memberId ).operatorState() == STARTED;
    }

    private static ServerInfo newServerInfo( Map.Entry<MemberId,? extends DiscoveryServerInfo> entry )
    {
        return newServerInfo( entry.getKey(), entry.getValue() );
    }

    private static ServerInfo newServerInfo( MemberId memberId, DiscoveryServerInfo discoveryServerInfo )
    {
        return new ServerInfo( discoveryServerInfo.connectors().clientBoltAddress(), memberId, discoveryServerInfo.groups() );
    }
}
