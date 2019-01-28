/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.RoutingResult;

import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.values.virtual.MapValue;

public interface LoadBalancingProcessor
{
    /**
     * Runs the procedure using the supplied client context
     * and returns the result.
     *
     * @param context The client supplied context.
     * @return The result of invoking the procedure.
     */
    Result run( MapValue context ) throws ProcedureException;

    interface Result extends RoutingResult
    {
        /**
         * @return List of ROUTE-capable endpoints.
         */
        List<Endpoint> routeEndpoints();

        /**
         * @return List of WRITE-capable endpoints.
         */
        List<Endpoint> writeEndpoints();

        /**
         * @return List of READ-capable endpoints.
         */
        List<Endpoint> readEndpoints();
    }
}
