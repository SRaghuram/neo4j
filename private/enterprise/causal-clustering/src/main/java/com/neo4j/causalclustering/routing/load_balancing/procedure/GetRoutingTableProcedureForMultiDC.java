/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.procedure.builtin.routing.BaseGetRoutingTableProcedure;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

/**
 * Returns endpoints and their capabilities.
 *
 * V2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetRoutingTableProcedureForMultiDC extends BaseGetRoutingTableProcedure
{
    private static final String DESCRIPTION = "Returns cluster endpoints and their capabilities.";

    private final LoadBalancingProcessor loadBalancingProcessor;

    public GetRoutingTableProcedureForMultiDC( List<String> namespace, LoadBalancingProcessor loadBalancingProcessor, DatabaseIdRepository databaseIdRepository,
            Config config )
    {
        super( namespace, databaseIdRepository, config );
        this.loadBalancingProcessor = loadBalancingProcessor;
    }

    @Override
    protected String description()
    {
        return DESCRIPTION;
    }

    @Override
    protected RoutingResult invoke( DatabaseId databaseId, MapValue routingContext ) throws ProcedureException
    {
        return loadBalancingProcessor.run( databaseId.name(), routingContext );
    }
}
