/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.procedure.builtin.routing.RoutingResult;

import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_allow_reads_on_leader;
import static java.util.stream.Collectors.toList;

public class AddressCollector
{
    private final LeaderService leaderService;
    private final Config config;
    private final Log log;
    private final Function<NamedDatabaseId,ClusterServerInfos> infosProvider;

    public AddressCollector( Function<NamedDatabaseId,ClusterServerInfos> infosProvider, LeaderService leaderService, Config config, Log log )
    {
        this.infosProvider = infosProvider;
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
        var allServerInfos = infosProvider.apply( namedDatabaseId );

        var timeToLive = config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
        var shouldShuffle = config.get( CausalClusteringSettings.load_balancing_shuffle );

        return new RoutingResult( routeEndpoints( allServerInfos, policy, shouldShuffle ),
                writeEndpoints( namedDatabaseId ),
                readEndpoints( allServerInfos, policy, shouldShuffle, namedDatabaseId ),
                timeToLive );
    }

    private List<SocketAddress> routeEndpoints( ClusterServerInfos clusterServerInfos, Policy policy, boolean shouldShuffle )
    {
        if ( policy == null )
        {
            var cores = clusterServerInfos.cores().allServers().stream()
                    .map( ServerInfo::boltAddress )
                    .collect( toList() );
            Collections.shuffle( cores );
            return cores;
        }

        var cores = clusterServerInfos.cores().allServers();

        var preferredRouters = new ArrayList<>( policy.apply( cores ) );
        var otherRouters = new ArrayList<>( cores );
        otherRouters.removeAll( preferredRouters );

        if ( shouldShuffle )
        {
            Collections.shuffle( preferredRouters );
            Collections.shuffle( otherRouters );
        }

        return Stream.concat( preferredRouters.stream(), otherRouters.stream() ).map( ServerInfo::boltAddress ).collect( toList() );
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

    private List<SocketAddress> readEndpoints( ClusterServerInfos clusterServerInfos, Policy policy, Boolean shouldShuffle, NamedDatabaseId namedDatabaseId )
    {
        var allowReadOnFollowers = config.get( cluster_allow_reads_on_followers );
        var allowReadOnLeader = config.get( cluster_allow_reads_on_leader );
        var possibleReaders = new HashSet<>( clusterServerInfos.readReplicas().onlineServers() );

        if ( allowReadOnFollowers || possibleReaders.isEmpty() )
        {
            possibleReaders.addAll( clusterServerInfos.followers().onlineServers() );
        }
        if ( allowReadOnLeader || possibleReaders.isEmpty() )
        {
            possibleReaders.addAll( clusterServerInfos.leader().onlineServers() );
        }

        var readers = new ArrayList<>( policy == null ? possibleReaders :
                                       policy.apply( possibleReaders ) );

        if ( shouldShuffle || policy == null )
        {
            Collections.shuffle( readers );
        }
        return readers.stream().map( ServerInfo::boltAddress ).collect( toList() );
    }
}
