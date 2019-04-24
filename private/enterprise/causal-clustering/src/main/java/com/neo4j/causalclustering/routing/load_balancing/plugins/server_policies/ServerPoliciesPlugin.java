/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.DatabaseId;
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
    private boolean shouldShuffle;

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
    public RoutingResult run( String databaseName, MapValue context ) throws ProcedureException
    {
        var dbId = new DatabaseId( databaseName );
        Policy policy = policies.selectFor( context );

        DatabaseCoreTopology coreTopology = coreTopologyFor( dbId );
        DatabaseReadReplicaTopology rrTopology = readReplicaTopology( dbId );

        return new RoutingResult( routeEndpoints( coreTopology, policy ), writeEndpoints( dbId ),
                readEndpoints( coreTopology, rrTopology, policy, dbId ), timeToLive );
    }

    private List<AdvertisedSocketAddress> routeEndpoints( DatabaseCoreTopology coreTopology, Policy policy )
    {
        Set<ServerInfo> routers = coreTopology.members().entrySet().stream()
                .map( e ->
                {
                    MemberId m = e.getKey();
                    CoreServerInfo c = e.getValue();
                    return new ServerInfo( c.connectors().boltAddress(), m, c.groups() );
                } ).collect( Collectors.toSet() );

        Set<ServerInfo> preferredRouters = policy.apply( routers );
        List<ServerInfo> otherRouters = routers.stream().filter( r -> !preferredRouters.contains( r ) ).collect( Collectors.toList() );
        List<ServerInfo> preferredRoutersList = new ArrayList<>( preferredRouters );

        if ( shouldShuffle )
        {
            Collections.shuffle( preferredRoutersList );
            Collections.shuffle( otherRouters );
        }

        return Stream.concat( preferredRouters.stream(), otherRouters.stream() )
                .map( ServerInfo::boltAddress ).collect( Collectors.toList() );
    }

    private List<AdvertisedSocketAddress> writeEndpoints( DatabaseId databaseId )
    {
        return leaderService.getLeaderBoltAddress( databaseId ).map( List::of ).orElse( emptyList() );
    }

    private List<AdvertisedSocketAddress> readEndpoints( DatabaseCoreTopology coreTopology, DatabaseReadReplicaTopology rrTopology, Policy policy,
            DatabaseId databaseId )
    {

        Set<ServerInfo> possibleReaders = rrTopology.members().entrySet().stream()
                .map( entry -> new ServerInfo( entry.getValue().connectors().boltAddress(), entry.getKey(),
                        entry.getValue().groups() ) )
                .collect( Collectors.toSet() );

        if ( allowReadsOnFollowers || possibleReaders.isEmpty() )
        {
            Map<MemberId,CoreServerInfo> coreMembers = coreTopology.members();
            Set<MemberId> validCores = coreMembers.keySet();

            Optional<MemberId> optionalLeaderId = leaderService.getLeaderId( databaseId );
            if ( optionalLeaderId.isPresent() )
            {
                MemberId leaderId = optionalLeaderId.get();
                validCores = validCores.stream().filter( memberId -> !memberId.equals( leaderId ) ).collect( Collectors.toSet() );
            }
            // leader might become available a bit later and we might end up using it for reading during this ttl, should be fine in general

            for ( MemberId validCore : validCores )
            {
                CoreServerInfo coreServerInfo = coreMembers.get( validCore );
                if ( coreServerInfo != null )
                {
                    possibleReaders.add( new ServerInfo( coreServerInfo.connectors().boltAddress(), validCore, coreServerInfo.groups() ) );
                }
            }
        }

        List<ServerInfo> readers = new ArrayList<>( policy.apply( possibleReaders ) );

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
}
