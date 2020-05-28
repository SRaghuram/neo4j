/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

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
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.NamedDatabaseId;

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
    public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return filters
                .flatMap( filters -> choices( namedDatabaseId, filters )
                .findFirst() );
    }

    @Override
    public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return filters.stream()
                .flatMap( filters -> choices( namedDatabaseId, filters ) )
                .collect( Collectors.toList() );
    }

    private Stream<MemberId> choices( NamedDatabaseId namedDatabaseId, Filter<ServerInfo> filter )
    {
        Set<ServerInfo> possibleServers = possibleServers( namedDatabaseId );

        return filter.apply( possibleServers ).stream().map( ServerInfo::memberId ).filter( memberId -> !Objects.equals( myself, memberId ) );
    }

    private Set<ServerInfo> possibleServers( NamedDatabaseId namedDatabaseId )
    {
        DatabaseCoreTopology coreTopology = topologyService.coreTopologyForDatabase( namedDatabaseId );
        DatabaseReadReplicaTopology readReplicaTopology = topologyService.readReplicaTopologyForDatabase( namedDatabaseId );

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
        return new ServerInfo( server.connectors().clientBoltAddress(), memberId, server.groups() );
    }
}
