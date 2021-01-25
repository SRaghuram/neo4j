/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;

import java.util.Collections;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

/**
 * Shuffles the servers of the delegate around so that every client
 * invocation gets a a little bit of that extra entropy spice.
 *
 * N.B: Lists are shuffled in place.
 */
public class ServerShufflingProcessor implements LoadBalancingProcessor
{
    private final LoadBalancingProcessor delegate;

    public ServerShufflingProcessor( LoadBalancingProcessor delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public RoutingResult run( NamedDatabaseId namedDatabaseId, MapValue context ) throws ProcedureException
    {
        RoutingResult result = delegate.run( namedDatabaseId, context );

        Collections.shuffle( result.routeEndpoints() );
        Collections.shuffle( result.writeEndpoints() );
        Collections.shuffle( result.readEndpoints() );

        return result;
    }

    public LoadBalancingProcessor delegate()
    {
        return delegate;
    }
}
