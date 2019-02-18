/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Util;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;

/**
 * The server policies plugin defines policies on the server-side which
 * can be bound to by a client by supplying a appropriately formed context.
 *
 * An example would be to define different policies for different regions.
 */
public class ServerPoliciesPlugin implements LoadBalancingPlugin
{
    public static final String PLUGIN_NAME = "server_policies";

    private TopologyService topologyService;
    private LeaderLocator leaderLocator;
    private Long timeToLive;
    private boolean allowReadsOnFollowers;
    private Policies policies;

    @Override
    public void validate( Config config, Log log ) throws InvalidSettingException
    {
        try
        {
            FilteringPolicyLoader.load( config, PLUGIN_NAME, log );
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
        this.leaderLocator = leaderLocator;
        this.timeToLive = config.get( CausalClusteringSettings.cluster_routing_ttl ).toMillis();
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
        this.policies = FilteringPolicyLoader.load( config, PLUGIN_NAME, logProvider.getLog( getClass() ) );
    }

    @Override
    public String pluginName()
    {
        return PLUGIN_NAME;
    }

    @Override
    public Result run( MapValue context ) throws ProcedureException
    {
        Policy policy = policies.selectFor( context );

        CoreTopology coreTopology = topologyService.localCoreServers();
        ReadReplicaTopology rrTopology = topologyService.localReadReplicas();

        return new LoadBalancingResult( routeEndpoints( coreTopology ), writeEndpoints( coreTopology ),
                readEndpoints( coreTopology, rrTopology, policy ), timeToLive );
    }

    private List<Endpoint> routeEndpoints( CoreTopology cores )
    {
        return cores.members().values().stream().map( Util.extractBoltAddress() )
                .map( Endpoint::route ).collect( Collectors.toList() );
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
                .map( Util.extractBoltAddress() )
                .map( Endpoint::write );

        return Util.asList( endPoint );
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

        Set<ServerInfo> readers = policy.apply( possibleReaders );
        return readers.stream().map( r -> Endpoint.read( r.boltAddress() ) ).collect( Collectors.toList() );
    }
}
