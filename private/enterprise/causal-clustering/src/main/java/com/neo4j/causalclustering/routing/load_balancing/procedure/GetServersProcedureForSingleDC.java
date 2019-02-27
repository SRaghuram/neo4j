/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseGetRoutingTableProcedure;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.AnyValue;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.causalclustering.routing.Util.asList;
import static com.neo4j.causalclustering.routing.Util.extractBoltAddress;
import static java.util.stream.Collectors.toList;

/**
 * Returns endpoints and their capabilities.
 *
 * GetServersV2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetServersProcedureForSingleDC extends BaseGetRoutingTableProcedure
{
    private static final String DESCRIPTION = "Returns cluster endpoints and their capabilities for single data center setup.";

    private final TopologyService topologyService;
    private final LeaderLocator leaderLocator;
    private final Config config;
    private final Log log;

    public GetServersProcedureForSingleDC( List<String> namespace, TopologyService topologyService, LeaderLocator leaderLocator,
            Config config, LogProvider logProvider )
    {
        super( namespace );
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected String description()
    {
        return DESCRIPTION;
    }

    @Override
    protected RoutingResult invoke( AnyValue[] input )
    {
        List<AdvertisedSocketAddress> routeEndpoints = routeEndpoints();
        List<AdvertisedSocketAddress> writeEndpoints = writeEndpoints();
        List<AdvertisedSocketAddress> readEndpoints = readEndpoints();

        long timeToLiveMillis = config.get( GraphDatabaseSettings.routing_ttl ).toMillis();

        return new RoutingResult( routeEndpoints, writeEndpoints, readEndpoints, timeToLiveMillis );
    }

    private Optional<AdvertisedSocketAddress> leaderBoltAddress()
    {
        MemberId leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            log.debug( "No leader server found. This can happen during a leader switch. No write end points available" );
            return Optional.empty();
        }

        return topologyService.localCoreServers().find( leader ).map( extractBoltAddress() );
    }

    private List<AdvertisedSocketAddress> routeEndpoints()
    {
        List<AdvertisedSocketAddress> routers = topologyService.localCoreServers()
                .members()
                .values()
                .stream()
                .map( extractBoltAddress() )
                .collect( toList() );

        Collections.shuffle( routers );
        return routers;
    }

    private List<AdvertisedSocketAddress> writeEndpoints()
    {
        return asList( leaderBoltAddress() );
    }

    private List<AdvertisedSocketAddress> readEndpoints()
    {
        List<AdvertisedSocketAddress> readReplicas = topologyService.localReadReplicas().allMemberInfo().stream()
                .map( extractBoltAddress() ).collect( toList() );
        boolean addFollowers = readReplicas.isEmpty() || config.get( cluster_allow_reads_on_followers );
        Stream<AdvertisedSocketAddress> readCore = addFollowers ? coreReadEndPoints() : Stream.empty();
        List<AdvertisedSocketAddress> readEndPoints = Stream.concat( readReplicas.stream(), readCore ).collect( toList() );
        Collections.shuffle( readEndPoints );
        return readEndPoints;
    }

    private Stream<AdvertisedSocketAddress> coreReadEndPoints()
    {
        Optional<AdvertisedSocketAddress> leader = leaderBoltAddress();
        Collection<CoreServerInfo> coreServerInfo = topologyService.localCoreServers().members().values();
        Stream<AdvertisedSocketAddress> boltAddresses = topologyService.localCoreServers()
                .members().values().stream().map( extractBoltAddress() );

        // if the leader is present and it is not alone filter it out from the read end points
        if ( leader.isPresent() && coreServerInfo.size() > 1 )
        {
            AdvertisedSocketAddress advertisedSocketAddress = leader.get();
            return boltAddresses.filter( address -> !advertisedSocketAddress.equals( address ) );
        }

        // if there is only the leader return it as read end point
        // or if we cannot locate the leader return all cores as read end points
        return boltAddresses;
    }
}
