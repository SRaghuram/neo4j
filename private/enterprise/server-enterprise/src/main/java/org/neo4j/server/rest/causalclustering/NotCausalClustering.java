/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.status;

class NotCausalClustering extends BaseStatus
{
    NotCausalClustering( OutputFormat output )
    {
        super( output );
    }

    @Override
    public Response discover()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response available()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response readonly()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response writable()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response description()
    {
        return Response.status( FORBIDDEN ).build();
    }
}
