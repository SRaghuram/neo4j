/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;
import static org.neo4j.causalclustering.routing.Util.asList;
import static org.neo4j.causalclustering.routing.Util.extractBoltAddress;
import static org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.FilteringPolicyLoader.load;

/**
 * The server policies plugin defines policies on the server-side which
 * can be bound to by a client by supplying a appropriately formed context.
 *
 * An example would be to define different policies for different regions.
 *
 * If so configured, this plugin also shuffles servers within each role
 * around so that every client invocation gets a a little bit of
 * that extra entropy spice.
 *
 * N.B: Lists are shuffled in place.
 */
@Service.Implementation( LoadBalancingPlugin.class )
public class ServerPoliciesPlugin implements LoadBalancingPlugin
{
    public static final String PLUGIN_NAME = "server_policies";

    private TopologyService topologyService;
    private LeaderLocator leaderLocator;
    private Long timeToLive;
    private boolean allowReadsOnFollowers;
    private Policies policies;
    private boolean shouldShuffle;

    @Override
    public void validate( Config config, Log log ) throws InvalidSettingException
    {
        try
        {
            load( config, PLUGIN_NAME, log );
        }
        catch ( InvalidFilterSpecification e )
        {
            throw new InvalidSettingException( "Invalid filter specification", e );
        }
    }

    @Override
    public void init( TopologyService topologyService, LeaderLocator leaderLocator,
            LogProvider logProvider, Config config ) throws InvalidFilterSpecification
    {
        this.topologyService = topologyService;
        this.shouldShuffle = config.get( CausalClusteringSettings.load_balancing_shuffle );
        this.leaderLocator = leaderLocator;
        this.timeToLive = config.get( CausalClusteringSettings.cluster_routing_ttl ).toMillis();
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
        this.policies = load( config, PLUGIN_NAME, logProvider.getLog( getClass() ) );
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
    public Result run( Map<String,String> context ) throws ProcedureException
    {
        Policy policy = policies.selectFor( context );

        CoreTopology coreTopology = topologyService.localCoreServers();
        ReadReplicaTopology rrTopology = topologyService.localReadReplicas();

        return new LoadBalancingResult( routeEndpoints( coreTopology, policy ), writeEndpoints( coreTopology ),
                readEndpoints( coreTopology, rrTopology, policy ), timeToLive );
    }

    private List<Endpoint> routeEndpoints( CoreTopology cores, Policy policy )
    {
        Set<ServerInfo> routers = cores.members().entrySet().stream()
                .map( e ->
                {
                    MemberId m = e.getKey();
                    CoreServerInfo c = e.getValue();
                    return new ServerInfo( c.connectors().boltAddress(), m, c.groups() );
                } ).collect( Collectors.toSet());

        Set<ServerInfo> preferredRouters = policy.apply( routers );
        List<ServerInfo> otherRouters = routers.stream().filter( r -> !preferredRouters.contains( r ) ).collect( Collectors.toList() );
        List<ServerInfo> preferredRoutersList = new ArrayList<>( preferredRouters );

        if ( shouldShuffle )
        {
            Collections.shuffle( preferredRoutersList );
            Collections.shuffle( otherRouters );
        }

        return Stream.concat( preferredRoutersList.stream(), otherRouters.stream() )
                .map( r -> Endpoint.route( r.boltAddress() ) ).collect( Collectors.toList() );
    }

    private List<Endpoint> writeEndpoints( CoreTopology cores )
    {

        MemberId leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            return emptyList();
        }

        Optional<Endpoint> endPoint = cores.find( leader )
                .map( extractBoltAddress() )
                .map( Endpoint::write );

        return asList( endPoint );
    }

    private List<Endpoint> readEndpoints( CoreTopology coreTopology, ReadReplicaTopology rrTopology, Policy policy )
    {

        Set<ServerInfo> possibleReaders = rrTopology.members().entrySet().stream()
                .map( entry -> new ServerInfo( entry.getValue().connectors().boltAddress(), entry.getKey(),
                        entry.getValue().groups() ) )
                .collect( Collectors.toSet() );

        if ( allowReadsOnFollowers || possibleReaders.size() == 0 )
        {
            Set<MemberId> validCores = coreTopology.members().keySet();
            try
            {
                MemberId leader = leaderLocator.getLeader();
                validCores = validCores.stream().filter( memberId -> !memberId.equals( leader ) ).collect( Collectors.toSet() );
            }
            catch ( NoLeaderFoundException ignored )
            {
                // we might end up using the leader for reading during this ttl, should be fine in general
            }

            for ( MemberId validCore : validCores )
            {
                Optional<CoreServerInfo> coreServerInfo = coreTopology.find( validCore );
                coreServerInfo.ifPresent(
                        coreServerInfo1 -> possibleReaders.add(
                                new ServerInfo( coreServerInfo1.connectors().boltAddress(), validCore, coreServerInfo1.groups() ) ) );
            }
        }

        List<ServerInfo> readers = new ArrayList<>( policy.apply( possibleReaders ) );

        if ( shouldShuffle )
        {
            Collections.shuffle( readers );
        }
        return readers.stream().map( r -> Endpoint.read( r.boltAddress() ) ).collect( Collectors.toList() );
    }
}
