/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins;

import java.util.Collections;
import java.util.Map;

import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.causalclustering.routing.load_balancing.ShufflingPlugin;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

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
    public Result run( Map<String,String> context ) throws ProcedureException
    {
        Result result = delegate.run( context );

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
