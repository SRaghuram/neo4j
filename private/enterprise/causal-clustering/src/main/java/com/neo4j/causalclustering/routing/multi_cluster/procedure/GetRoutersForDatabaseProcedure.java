/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.procedure.Mode;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static com.neo4j.causalclustering.routing.Util.extractBoltAddress;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.DATABASE;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.ROUTERS;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_DATABASE;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.procedure.builtin.routing.ParameterNames.TTL;

public class GetRoutersForDatabaseProcedure implements CallableProcedure
{
    private static final String DESCRIPTION = "Returns router capable endpoints for a specific database in a multi-cluster.";

    private final ProcedureSignature procedureSignature =
            procedureSignature( GET_ROUTERS_FOR_DATABASE.fullyQualifiedName() )
                    .in( DATABASE.parameterName(), Neo4jTypes.NTString )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( ROUTERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .mode( Mode.DBMS )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final long timeToLiveMillis;

    public GetRoutersForDatabaseProcedure( TopologyService topologyService, Config config )
    {
        this.topologyService = topologyService;
        this.timeToLiveMillis = config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
    }

    @Override
    public ProcedureSignature signature()
    {
        return procedureSignature;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        String dbName = ((TextValue) input[0]).stringValue();
        List<AdvertisedSocketAddress> routers = routeEndpoints( dbName );

        Map<String,List<AdvertisedSocketAddress>> routerMap = new HashMap<>();
        routerMap.put( dbName, routers );

        MultiClusterRoutingResult result = new MultiClusterRoutingResult( routerMap, timeToLiveMillis );
        return RawIterator.<AnyValue[], ProcedureException>of( MultiClusterRoutingResultFormat.build( result ) );
    }

    private List<AdvertisedSocketAddress> routeEndpoints( String dbName )
    {
        CoreTopology filtered = topologyService.allCoreServers().filterTopologyByDb( dbName );
        Stream<CoreServerInfo> filteredCoreMemberInfo = filtered.members().values().stream();

        return filteredCoreMemberInfo.map( extractBoltAddress() ).collect( Collectors.toList() );
    }
}
