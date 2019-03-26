/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

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
    public void init( TopologyService topologyService, LeaderService leaderService,
            LogProvider logProvider, Config config ) throws InvalidFilterSpecification
    {
        this.topologyService = topologyService;
        this.leaderService = leaderService;
        this.timeToLive = config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
        this.policies = FilteringPolicyLoader.load( config, PLUGIN_NAME, logProvider.getLog( getClass() ) );
    }

    @Override
    public String pluginName()
    {
        return PLUGIN_NAME;
    }

    @Override
    public RoutingResult run( String databaseName, MapValue context ) throws ProcedureException
    {
        Policy policy = policies.selectFor( context );

        CoreTopology coreTopology = coreTopologyFor( databaseName );
        ReadReplicaTopology rrTopology = readReplicaTopology( databaseName );

        return new RoutingResult( routeEndpoints( coreTopology ), writeEndpoints( databaseName ),
                readEndpoints( coreTopology, rrTopology, policy, databaseName ), timeToLive );
    }

    private static List<AdvertisedSocketAddress> routeEndpoints( CoreTopology coreTopology )
    {
        return coreTopology.members()
                .values()
                .stream()
                .map( ClientConnector::boltAddress )
                .collect( toList() );
    }

    private List<AdvertisedSocketAddress> writeEndpoints( String databaseName )
    {
        return leaderService.getLeaderBoltAddress( databaseName ).map( List::of ).orElse( emptyList() );
    }

    private List<AdvertisedSocketAddress> readEndpoints( CoreTopology coreTopology, ReadReplicaTopology rrTopology, Policy policy, String databaseName )
    {

        Set<ServerInfo> possibleReaders = rrTopology.members().entrySet().stream()
                .map( entry -> new ServerInfo( entry.getValue().connectors().boltAddress(), entry.getKey(),
                        entry.getValue().groups() ) )
                .collect( Collectors.toSet() );

        if ( allowReadsOnFollowers || possibleReaders.size() == 0 )
        {
            Set<MemberId> validCores = coreTopology.members().keySet();

            Optional<MemberId> optionalLeaderId = leaderService.getLeaderId( databaseName );
            if ( optionalLeaderId.isPresent() )
            {
                MemberId leaderId = optionalLeaderId.get();
                validCores = validCores.stream().filter( memberId -> !memberId.equals( leaderId ) ).collect( Collectors.toSet() );
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
        return readers.stream().map( ServerInfo::boltAddress ).collect( toList() );
    }

    private CoreTopology coreTopologyFor( String databaseName )
    {
        // todo: filtering needs to be enabled once discovery contains multi-db and not multi-clustering database names
        //  also an exception needs to be thrown when topology for the specified database is empty
        // return topologyService.allCoreServers().filterTopologyByDb( databaseName );
        return topologyService.allCoreServers();
    }

    private ReadReplicaTopology readReplicaTopology( String databaseName )
    {
        // todo: filtering needs to be enabled once discovery contains multi-db and not multi-clustering database names
        // return topologyService.allReadReplicas().filterTopologyByDb( databaseName );
        return topologyService.allReadReplicas();
    }
}
