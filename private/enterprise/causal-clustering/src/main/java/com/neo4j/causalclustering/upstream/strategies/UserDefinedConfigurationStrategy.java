/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.FilterConfigParser;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.InvalidFilterSpecification;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerInfo;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.DatabaseId;

@ServiceProvider
public class UserDefinedConfigurationStrategy extends UpstreamDatabaseSelectionStrategy
{

    public static final String IDENTITY = "user-defined";
    // Empty if provided filter config cannot be parsed.
    // Ideally this class would not be created until config has been successfully parsed
    // in which case there would be no need for Optional
    private Optional<Filter<ServerInfo>> filters;

    public UserDefinedConfigurationStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        String filterConfig = config.get( CausalClusteringSettings.user_defined_upstream_selection_strategy );
        try
        {
            Filter<ServerInfo> parsed = FilterConfigParser.parse( filterConfig );
            filters = Optional.of( parsed );
            log.info( "Upstream selection strategy " + name + " configured with " + filterConfig );
        }
        catch ( InvalidFilterSpecification invalidFilterSpecification )
        {
            filters = Optional.empty();
            log.warn( "Cannot parse configuration '" + filterConfig + "' for upstream selection strategy " + name + ". " +
                    invalidFilterSpecification.getMessage() );
        }
    }

    @Override
    public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
    {
        return filters.flatMap( filters ->
        {
            Set<ServerInfo> possibleServers = possibleServers( databaseId );

            return filters.apply( possibleServers ).stream().map( ServerInfo::memberId ).filter( memberId -> !Objects.equals( myself, memberId ) ).findFirst();
        } );
    }

    private Set<ServerInfo> possibleServers( DatabaseId databaseId )
    {
        DatabaseCoreTopology coreTopology = topologyService.coreTopologyForDatabase( databaseId );
        DatabaseReadReplicaTopology readReplicaTopology = topologyService.readReplicaTopologyForDatabase( databaseId );

        var infoMap = Stream.of( coreTopology, readReplicaTopology )
                .map( Topology::members )
                .map( Map::entrySet )
                .flatMap( Set::stream );

        return infoMap.map( this::toServerInfo ).collect( Collectors.toSet() );
    }

    private <T extends DiscoveryServerInfo> ServerInfo toServerInfo( Map.Entry<MemberId,T> entry )
    {
        T server = entry.getValue();
        MemberId memberId = entry.getKey();
        return new ServerInfo( server.connectors().boltAddress(), memberId, server.groups() );
    }
}
