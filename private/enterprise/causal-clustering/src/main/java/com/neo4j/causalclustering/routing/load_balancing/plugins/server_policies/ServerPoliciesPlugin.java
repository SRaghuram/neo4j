/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * The server policies plugin defines policies on the server-side which
 * can be bound to by a client by supplying a appropriately formed context.
 *
 * An example would be to define different policies for different regions.
 */
@ServiceProvider
public class ServerPoliciesPlugin implements LoadBalancingPlugin
{
    public static final String PLUGIN_NAME = "server_policies";

    private TopologyService topologyService;
    private LeaderService leaderService;
    private Long timeToLive;
    private boolean allowReadsOnFollowers;
    private Policies policies;
    private boolean shouldShuffle;

    @Override
    public void validate( Config config, Log log )
    {
        FilteringPolicyLoader.loadServerPolicies( config, log );
    }

    @Override
    public void init( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
    {
        this.topologyService = topologyService;
        this.leaderService = leaderService;
        this.timeToLive = config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
        this.policies = FilteringPolicyLoader.loadServerPolicies( config, logProvider.getLog( getClass() ) );
        this.shouldShuffle = config.get( CausalClusteringSettings.load_balancing_shuffle );
    }

    @Override
    public String pluginName()
    {
        return PLUGIN_NAME;
    }

    @Override
    public boolean isShufflingPlugin()
    {
        return true;
    }

    @Override
    public RoutingResult run( DatabaseId databaseId, MapValue context ) throws ProcedureException
    {
        var policy = policies.selectFor( context );

        var coreTopology = coreTopologyFor( databaseId );
        var rrTopology = readReplicaTopology( databaseId );

        return new RoutingResult( routeEndpoints( coreTopology, policy ), writeEndpoints( databaseId ),
                readEndpoints( coreTopology, rrTopology, policy, databaseId ), timeToLive );
    }

    private List<SocketAddress> routeEndpoints( DatabaseCoreTopology coreTopology, Policy policy )
    {
        var routers = coreTopology.members().entrySet().stream()
                .map( ServerPoliciesPlugin::newServerInfo ).collect( toSet() );

        var preferredRouters = policy.apply( routers );
        var otherRoutersList = routers.stream().filter( not( preferredRouters::contains ) ).collect( toList() );
        var preferredRoutersList = new ArrayList<>( preferredRouters );

        if ( shouldShuffle )
        {
            Collections.shuffle( preferredRoutersList );
            Collections.shuffle( otherRoutersList );
        }

        return Stream.concat( preferredRoutersList.stream(), otherRoutersList.stream() )
                .map( ServerInfo::boltAddress ).collect( toList() );
    }

    private List<SocketAddress> writeEndpoints( DatabaseId databaseId )
    {
        return leaderService.getLeaderBoltAddress( databaseId ).map( List::of ).orElse( emptyList() );
    }

    private List<SocketAddress> readEndpoints( DatabaseCoreTopology coreTopology, DatabaseReadReplicaTopology rrTopology, Policy policy,
            DatabaseId databaseId )
    {
        var possibleReaders = rrTopology.members().entrySet().stream()
                .map( ServerPoliciesPlugin::newServerInfo )
                .collect( toSet() );

        if ( allowReadsOnFollowers || possibleReaders.isEmpty() )
        {
            var coreMembers = coreTopology.members();
            var validCores = coreMembers.keySet();

            var optionalLeaderId = leaderService.getLeaderId( databaseId );
            if ( optionalLeaderId.isPresent() )
            {
                var leaderId = optionalLeaderId.get();
                validCores = validCores.stream().filter( memberId -> !memberId.equals( leaderId ) ).collect( toSet() );
            }
            // leader might become available a bit later and we might end up using it for reading during this ttl, should be fine in general

            for ( var validCore : validCores )
            {
                var coreServerInfo = coreMembers.get( validCore );
                if ( coreServerInfo != null )
                {
                    possibleReaders.add( newServerInfo( validCore, coreServerInfo ) );
                }
            }
        }

        var readers = new ArrayList<>( policy.apply( possibleReaders ) );

        if ( shouldShuffle )
        {
            Collections.shuffle( readers );
        }
        return readers.stream().map( ServerInfo::boltAddress ).collect( toList() );
    }

    private DatabaseCoreTopology coreTopologyFor( DatabaseId databaseId )
    {
        return topologyService.coreTopologyForDatabase( databaseId );
    }

    private DatabaseReadReplicaTopology readReplicaTopology( DatabaseId databaseId )
    {
        return topologyService.readReplicaTopologyForDatabase( databaseId );
    }

    private static ServerInfo newServerInfo( Map.Entry<MemberId,? extends DiscoveryServerInfo> entry )
    {
        return newServerInfo( entry.getKey(), entry.getValue() );
    }

    private static ServerInfo newServerInfo( MemberId memberId, DiscoveryServerInfo discoveryServerInfo )
    {
        return new ServerInfo( discoveryServerInfo.connectors().boltAddress(), memberId, discoveryServerInfo.groups() );
    }
}
