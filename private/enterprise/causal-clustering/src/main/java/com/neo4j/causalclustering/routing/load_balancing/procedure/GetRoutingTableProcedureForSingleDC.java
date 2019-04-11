/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.discovery.ClientConnector;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseGetRoutingTableProcedure;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;

/**
 * Returns endpoints and their capabilities.
 *
 * V2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetRoutingTableProcedureForSingleDC extends BaseGetRoutingTableProcedure
{
    private static final String DESCRIPTION = "Returns cluster endpoints and their capabilities for single data center setup.";

    private final TopologyService topologyService;
    private final LeaderService leaderService;
    private final Log log;

    public GetRoutingTableProcedureForSingleDC( List<String> namespace, TopologyService topologyService, LeaderService leaderService,
            Config config, LogProvider logProvider )
    {
        super( namespace, config );
        this.topologyService = topologyService;
        this.leaderService = leaderService;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected String description()
    {
        return DESCRIPTION;
    }

    @Override
    protected RoutingResult invoke( DatabaseId databaseId, MapValue routingContext )
    {
        var routeEndpoints = routeEndpoints( databaseId );
        var writeEndpoints = writeEndpoints( databaseId );
        var readEndpoints = readEndpoints( databaseId );

        var timeToLiveMillis = config.get( routing_ttl ).toMillis();

        return new RoutingResult( routeEndpoints, writeEndpoints, readEndpoints, timeToLiveMillis );
    }

    private List<AdvertisedSocketAddress> routeEndpoints( DatabaseId databaseId )
    {
        var routers = coreTopologyFor( databaseId )
                .allMemberInfo()
                .map( ClientConnector::boltAddress )
                .collect( toList() );

        Collections.shuffle( routers );
        return routers;
    }

    private List<AdvertisedSocketAddress> writeEndpoints( DatabaseId databaseId )
    {
        var optionalLeaderAddress = leaderService.getLeaderBoltAddress( databaseId );
        if ( optionalLeaderAddress.isEmpty() )
        {
            log.debug( "No leader server found. This can happen during a leader switch. No write end points available" );
        }
        return optionalLeaderAddress.stream().collect( toList() );
    }

    private List<AdvertisedSocketAddress> readEndpoints( DatabaseId databaseId )
    {
        var readReplicas = readReplicaTopology( databaseId )
                .allMemberInfo()
                .map( ClientConnector::boltAddress )
                .collect( toList() );

        var allowReadsOnFollowers = readReplicas.isEmpty() || config.get( cluster_allow_reads_on_followers );
        var coreReadEndPoints = allowReadsOnFollowers ? coreReadEndPoints( databaseId ) : Stream.<AdvertisedSocketAddress>empty();
        var readEndPoints = Stream.concat( readReplicas.stream(), coreReadEndPoints ).collect( toList() );
        Collections.shuffle( readEndPoints );
        return readEndPoints;
    }

    private Stream<AdvertisedSocketAddress> coreReadEndPoints( DatabaseId databaseId )
    {
        var optionalLeaderAddress = leaderService.getLeaderBoltAddress( databaseId );
        var coreServerInfos = coreTopologyFor( databaseId ).members().values();
        var coreAddresses = coreServerInfos.stream().map( ClientConnector::boltAddress );

        // if the leader is present and it is not alone filter it out from the read end points
        if ( optionalLeaderAddress.isPresent() && coreServerInfos.size() > 1 )
        {
            var leaderAddress = optionalLeaderAddress.get();
            return coreAddresses.filter( address -> !leaderAddress.equals( address ) );
        }

        // if there is only the leader return it as read end point
        // or if we cannot locate the leader return all cores as read end points
        return coreAddresses;
    }

    private CoreTopology coreTopologyFor( DatabaseId databaseId )
    {
        // todo: filtering needs to be enabled once discovery contains multi-db and not multi-clustering database names
        //  also an exception needs to be thrown when topology for the specified database is empty
        // return topologyService.allCoreServers().filterTopologyByDb( databaseId );
        return topologyService.allCoreServers();
    }

    private ReadReplicaTopology readReplicaTopology( DatabaseId databaseId )
    {
        // todo: filtering needs to be enabled once discovery contains multi-db and not multi-clustering database names
        // return topologyService.allReadReplicas().filterTopologyByDb( databaseId );
        return topologyService.allReadReplicas();
    }
}
