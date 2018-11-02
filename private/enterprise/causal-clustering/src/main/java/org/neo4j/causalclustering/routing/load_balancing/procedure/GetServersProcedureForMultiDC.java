/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.procedure;

import java.util.Map;

import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;

import static org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames.CONTEXT;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames.SERVERS;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames.TTL;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ProcedureNames.GET_SERVERS_V2;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

/**
 * Returns endpoints and their capabilities.
 *
 * GetServersV2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetServersProcedureForMultiDC implements CallableProcedure
{
    private final String DESCRIPTION = "Returns cluster endpoints and their capabilities.";

    private final ProcedureSignature procedureSignature =
            procedureSignature( GET_SERVERS_V2.fullyQualifiedProcedureName() )
                    .in( CONTEXT.parameterName(), Neo4jTypes.NTMap )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( SERVERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .description( DESCRIPTION )
                    .build();

    private final LoadBalancingProcessor loadBalancingProcessor;

    public GetServersProcedureForMultiDC( LoadBalancingProcessor loadBalancingProcessor )
    {
        this.loadBalancingProcessor = loadBalancingProcessor;
    }

    @Override
    public ProcedureSignature signature()
    {
        return procedureSignature;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply(
            Context ctx, Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        @SuppressWarnings( "unchecked" )
        Map<String,String> clientContext = (Map<String,String>) input[0];

        LoadBalancingProcessor.Result result = loadBalancingProcessor.run( clientContext );

        return RawIterator.<Object[],ProcedureException>of( ResultFormatV1.build( result ) );
    }
}
