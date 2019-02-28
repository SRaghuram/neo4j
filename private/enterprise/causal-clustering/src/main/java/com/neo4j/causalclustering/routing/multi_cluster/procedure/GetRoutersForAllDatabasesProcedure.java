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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;

import static com.neo4j.causalclustering.routing.Util.extractBoltAddress;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.ROUTERS;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_ALL_DATABASES;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.builtinprocs.routing.ParameterNames.TTL;

public class GetRoutersForAllDatabasesProcedure implements CallableProcedure
{

    private static final String DESCRIPTION = "Returns router capable endpoints for each database name in a multi-cluster.";

    private final ProcedureSignature procedureSignature =
            procedureSignature( GET_ROUTERS_FOR_ALL_DATABASES.fullyQualifiedName() )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( ROUTERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .mode( Mode.DBMS )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final long timeToLiveMillis;

    public GetRoutersForAllDatabasesProcedure( TopologyService topologyService, Config config )
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
        Map<String,List<AdvertisedSocketAddress>> routersPerDb = routeEndpoints();
        MultiClusterRoutingResult result = new MultiClusterRoutingResult( routersPerDb, timeToLiveMillis );
        return RawIterator.<AnyValue[], ProcedureException>of( MultiClusterRoutingResultFormat.build( result ) );
    }

    private Map<String,List<AdvertisedSocketAddress>> routeEndpoints()
    {
        CoreTopology core =  topologyService.allCoreServers();
        Stream<CoreServerInfo> allCoreMemberInfo = topologyService.allCoreServers().members().values().stream();

        Map<String, List<CoreServerInfo>> coresByDb = allCoreMemberInfo.collect( Collectors.groupingBy( CoreServerInfo::getDatabaseName ) );

        Function<Map.Entry<String,List<CoreServerInfo>>,List<AdvertisedSocketAddress>> extractQualifiedBoltAddresses = entry ->
        {
            List<CoreServerInfo> cores = entry.getValue();
            return cores.stream().map( extractBoltAddress() ).collect( Collectors.toList() );
        };

        return coresByDb.entrySet().stream().collect( Collectors.toMap( Map.Entry::getKey, extractQualifiedBoltAddresses ) );
    }
}
