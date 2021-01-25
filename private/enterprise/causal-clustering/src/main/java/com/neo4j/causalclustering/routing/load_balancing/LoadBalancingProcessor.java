/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

public interface LoadBalancingProcessor
{
    /**
     * Runs the procedure using the supplied client context
     * and returns the result.
     *
     * @param namedDatabaseId the ID of the database.
     * @param context the client supplied context.
     * @return the result of invoking the procedure.
     */
    RoutingResult run( NamedDatabaseId namedDatabaseId, MapValue context ) throws ProcedureException;
}
