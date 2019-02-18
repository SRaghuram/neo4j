/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Util;
import com.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

public class GetRoutersForDatabaseProcedure implements CallableProcedure
{
    private static final String DESCRIPTION = "Returns router capable endpoints for a specific database in a multi-cluster.";

    private final ProcedureSignature procedureSignature =
            ProcedureSignature.procedureSignature( ProcedureNames.GET_ROUTERS_FOR_DATABASE.fullyQualifiedProcedureName() )
                    .in( ParameterNames.DATABASE.parameterName(), Neo4jTypes.NTString )
                    .out( ParameterNames.TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( ParameterNames.ROUTERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final long timeToLiveMillis;

    public GetRoutersForDatabaseProcedure( TopologyService topologyService, Config config )
    {
        this.topologyService = topologyService;
        this.timeToLiveMillis = config.get( CausalClusteringSettings.cluster_routing_ttl ).toMillis();
    }

    @Override
    public ProcedureSignature signature()
    {
        return procedureSignature;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        @SuppressWarnings( "unchecked" )
        String dbName = ((TextValue) input[0]).stringValue();
        List<Endpoint> routers = routeEndpoints( dbName );

        HashMap<String,List<Endpoint>> routerMap = new HashMap<>();
        routerMap.put( dbName, routers );

        MultiClusterRoutingResult result = new MultiClusterRoutingResult( routerMap, timeToLiveMillis );
        return RawIterator.<AnyValue[], ProcedureException>of( MultiClusterRoutingResultFormat.build( result ) );
    }

    private List<Endpoint> routeEndpoints( String dbName )
    {
        CoreTopology filtered = topologyService.allCoreServers().filterTopologyByDb( dbName );
        Stream<CoreServerInfo> filteredCoreMemberInfo = filtered.members().values().stream();

        return filteredCoreMemberInfo.map( Util.extractBoltAddress() ).map( Endpoint::route ).collect( Collectors.toList() );
    }
}
