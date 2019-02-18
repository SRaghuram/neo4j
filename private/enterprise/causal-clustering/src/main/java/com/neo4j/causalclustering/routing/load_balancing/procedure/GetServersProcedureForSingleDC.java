/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Util;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.AnyValue;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

/**
 * Returns endpoints and their capabilities.
 *
 * GetServersV2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetServersProcedureForSingleDC implements CallableProcedure
{
    private final String DESCRIPTION = "Returns cluster endpoints and their capabilities for single data center setup.";

    private final ProcedureSignature procedureSignature =
            ProcedureSignature.procedureSignature( ProcedureNames.GET_SERVERS_V2.fullyQualifiedProcedureName() )
                    .in( ParameterNames.CONTEXT.parameterName(), Neo4jTypes.NTMap )
                    .out( ParameterNames.TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( ParameterNames.SERVERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final LeaderLocator leaderLocator;
    private final Config config;
    private final Log log;

    public GetServersProcedureForSingleDC( TopologyService topologyService, LeaderLocator leaderLocator,
            Config config, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public ProcedureSignature signature()
    {
        return procedureSignature;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceTracker resourceTracker )
    {
        List<Endpoint> routeEndpoints = routeEndpoints();
        List<Endpoint> writeEndpoints = writeEndpoints();
        List<Endpoint> readEndpoints = readEndpoints();

        return RawIterator.<AnyValue[],ProcedureException>of( ResultFormatV1.build(
                new LoadBalancingResult( routeEndpoints, writeEndpoints, readEndpoints,
                        config.get( CausalClusteringSettings.cluster_routing_ttl ).toMillis() ) ) );
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

        return topologyService.localCoreServers().find( leader ).map( Util.extractBoltAddress() );
    }

    private List<Endpoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> routers = topologyService.localCoreServers()
                .members().values().stream().map( Util.extractBoltAddress() );
        List<Endpoint> routeEndpoints = routers.map( Endpoint::route ).collect( toList() );
        Collections.shuffle( routeEndpoints );
        return routeEndpoints;
    }

    private List<Endpoint> writeEndpoints()
    {
        return Util.asList( leaderBoltAddress().map( Endpoint::write ) );
    }

    private List<Endpoint> readEndpoints()
    {
        List<AdvertisedSocketAddress> readReplicas = topologyService.localReadReplicas().allMemberInfo().stream()
                .map( Util.extractBoltAddress() ).collect( toList() );
        boolean addFollowers = readReplicas.isEmpty() || config.get( cluster_allow_reads_on_followers );
        Stream<AdvertisedSocketAddress> readCore = addFollowers ? coreReadEndPoints() : Stream.empty();
        List<Endpoint> readEndPoints =
                concat( readReplicas.stream(), readCore ).map( Endpoint::read ).collect( toList() );
        Collections.shuffle( readEndPoints );
        return readEndPoints;
    }

    private Stream<AdvertisedSocketAddress> coreReadEndPoints()
    {
        Optional<AdvertisedSocketAddress> leader = leaderBoltAddress();
        Collection<CoreServerInfo> coreServerInfo = topologyService.localCoreServers().members().values();
        Stream<AdvertisedSocketAddress> boltAddresses = topologyService.localCoreServers()
                .members().values().stream().map( Util.extractBoltAddress() );

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
